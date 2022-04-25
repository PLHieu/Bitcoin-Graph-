https://btc.getblock.io/mainnet/

api_key: 
97774ee1-13b0-4545-a6c9-4b71cb567e81

bitcoinetl export_all --start 0 --end 500 --partition-batch-size 100 --provider-uri https://btc.getblock.io/mainnet/ --chain bitcoin

bitcoinetl export_blocks_and_transactions --start-block 0 --end-block 500 --batch-size 100 --provider-uri https://btc.getblock.io/mainnet/ --blocks-output blocks.json --transactions-output transactions.json

bitcoinetl enrich_transactions  --provider-uri https://btc.getblock.io/mainnet/ --transactions-input transactions_test.json --transactions-output enriched_transactions_test.json

