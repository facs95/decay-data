This repo is trying to efficiently collect migrated and merged accounts that were affected by the decay bug.

More info on the issue in this ticket [ticket](https://linear.app/evmos/issue/ENG-1509/early-decay-caused-loss-of-claimable-amount)

To accomplish this we are trying the following steps:

1. Iterate over blocks in which the decay bug was enabled. Lets say this period was between block `N` and `M`.
2. Query `block_results` on each block.
3. Iterate over all the txs in the block and their respective events.
4. Within this events search for the following even types:

- `merge_claims_records`
- `claim` if and only if the action is `ACTION_IBC_TRANSFER`.

5. Then we collect this data into two separate tables `merged_account` and `migrated_account`.

In order to run it please:

1. Modify the `FromBlock` and `ToBlock` value you want to iterate over.
2. Remove old `accounts.db` file if any.
3. Run `go run main.go`
