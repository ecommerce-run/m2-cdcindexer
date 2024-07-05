<?php

namespace Commerce\CDCIndexer\Console;

use Magento\Framework\App\DeploymentConfig;
use Magento\Framework\App\ObjectManager;
use Magento\Framework\Console\Cli;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class WatchCommand extends Command
{
    /**
     * @inheritdoc
     *
     * @param ?string $name
     */
    public function __construct(
        protected DeploymentConfig $deploymentConfig,
        $name = null,
    ) {
        parent::__construct($name);
    }
    /**
     * {@inheritdoc}
     */
    protected function configure()
    {
        $this->setName('indexer:cdc:watch');
        $this->setDescription('Watch stream and pass to the handler.');

        $this->addArgument(
            'cdc-configuration-name',
            InputArgument::REQUIRED,
            'ClassName of indexers you can find in the mview.xml,'
        );

        $this->setHelp(
            <<<HELP
Configuration is done via app/etc/env.php
    'cdc' => [
        'catalog_category_product' => [ // cdc-configuration-name reference name
            'handler' => 'Magento\Catalog\Model\Indexer\Category\Product::executeList', // Class and method that will handle
            'stream' => 'target.catalog_category_product', // Stream name to observe
            'delay' => 0, // Delay time in seconds
            'source' => [
                'host' => 'cdc-queue',
                'port' => '6379',
                'database' => '2',
                'count' => 2, // redis streams argument
                'block' => 500 // redis streams argument
            ]
        ]
    ]
To start watching stream:
      <comment>%command.full_name% cdc-configuration-name</comment>
HELP
        );
        parent::configure();

    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {

        $processor = $input->getArgument('cdc-configuration-name');

        $output->writeln('<info> Starting processor' . $processor . '</info>');
        $processorConfig = $this->deploymentConfig->get('cdc/' . $processor);
        list($classname, $methodname) = explode('::', $processorConfig['handler']);
        $handlerInstance = ObjectManager::getInstance()->get($classname);
        $streamIterator = $this->getStreamIterator($processorConfig['stream'], $processorConfig['source']);
        foreach ($streamIterator as $ids) {
            $handlerInstance->{$methodname}($ids);
        }
        return Cli::RETURN_SUCCESS;
    }

    protected function getStreamIterator($streamName, $connectionConfig)
    {
        $redis = new \Credis_Client($connectionConfig['host'], $connectionConfig['port'], db:$connectionConfig['database']);

        while (true) {

            $steamMessages = $redis->xread(
                'COUNT', $connectionConfig['count'] ?? 5,
                'BLOCK', $connectionConfig['block'] ?? 500,
                'STREAMS', $streamName, 0);
            if ($steamMessages)
                foreach ($steamMessages[0][1] as $message) {
                    list($messageID,$data) = $message;
                    $ids = json_decode($data[1],true);
                    if ($ids) {
                        yield $ids;
                    }
                    $redis->xdel($streamName, $messageID);
                }
        }
    }
}
