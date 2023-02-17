package main

import (
	"context"
	"database/sql"
	"fmt"
	dblib "github.com/facs95/decay-data/db"
	"github.com/facs95/decay-data/query"
	"log"
	"strconv"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

const (
	FromBlock  = 265401 // Block from which to start querying
	ToBlock    = 265402 // Last block to query
	BatchSize  = 1000   // Amount of blocks per thread
	MaxWorkers = 5      // Amount of threads
)

func main() {
	// Set up database connection
	db, err := sql.Open("sqlite3", "./accounts.db")
	if err != nil {
		log.Fatalf("Error opening database connection: %v", err)
	}
	defer db.Close()

	// Create en databases
	dblib.CreateMergeAccountTable(db)
	dblib.CreateMigrateAccountTable(db)

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Process the range in batches
	if err := handleWorkers(ctx, db, FromBlock, ToBlock, BatchSize, MaxWorkers); err != nil {
		log.Fatalf("Error processing range: %v", err)
	}
}

func handleWorkers(ctx context.Context, db *sql.DB, start, end, batchSize int, maxWorkers int) error {
	// Create a channel to hold jobs to be executed by workers
	jobs := make(chan []int, maxWorkers)
	// Create a WaitGroup to wait for all workers to complete
	wg := sync.WaitGroup{}

	// Launch worker goroutines
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobs {
				// Query the external resource for data
				mergedAccounts, migratedAccounts := processBatchOfBlocks(job)

				// Process the data and insert into MySQL database
				if err := insertIntoDB(ctx, db, migratedAccounts, mergedAccounts); err != nil {
					log.Printf("Error inserting into database: %v", err)
					continue
				}
			}
		}()
	}

	// Generate jobs for each batch and send them to the jobs channel
	for i := FromBlock; i < ToBlock; i += BatchSize {
		job := []int{i, i + BatchSize - 1}
		if job[1] > ToBlock {
			job[1] = ToBlock
		}

		jobs <- job
	}

	close(jobs)
	wg.Wait()

	return nil
}

func processBatchOfBlocks(job []int) ([]dblib.MergedAccount, []dblib.MigratedAccount) {
	mergedEvents, migratedEvents := []dblib.MergedAccount{}, []dblib.MigratedAccount{}
	for j := job[0]; j <= job[1]; j++ {
		blockResult, err := query.GetBlockResult(strconv.Itoa(j))
		if err != nil {
			// This should be on Error database
			log.Printf("Error querying external resource: %v", err)
			continue
		}
		merged, migrated := filterAndDecodeEvents(blockResult.Result.TxsResults, j)
		mergedEvents = append(mergedEvents, merged...)
		migratedEvents = append(migratedEvents, migrated...)
	}
	log.Printf("finished job for blocks: %v - %v", job[0], job[1])
	return mergedEvents, migratedEvents
}

func filterAndDecodeEvents(txs []query.ResponseDeliverTx, height int) ([]dblib.MergedAccount, []dblib.MigratedAccount) {
	mergedEvents, migratedEvents := []dblib.MergedAccount{}, []dblib.MigratedAccount{}
	//  Iterate over all txs in the block
	for i := range txs {
		// Iterate over all events in tx
		for index := range txs[i].Events {
			switch t := txs[i].Events[index].Type; t {
			case "merge_claims_records":
				v := txs[i].Events[index]
				// Decode the attributes
				err := v.DecodeAttributes()
				if err != nil {
					log.Printf("Error decoding resource: %v", err)
					// we should add this records to error table
					// return nil, nil
					continue
				}
				mergeRecord := dblib.MergedAccount{
					Height:            strconv.Itoa(height),
					Recipient:         v.Attributes[0].Value,
					ClaimedCoins:      v.Attributes[1].Value,
					FundCommunityPool: v.Attributes[2].Value,
				}
				mergedEvents = append(mergedEvents, mergeRecord)
				break
			case "claim":
				v := txs[i].Events[index]
				// Decode the attributes
				err := v.DecodeAttributes()
				if err != nil {
					log.Printf("Error decoding resource: %v", err)
					// we should add this records to error table
					// return nil, nil
					continue
				}
				migratedAccount := dblib.MigratedAccount{
					Height: strconv.Itoa(height),
					Sender: v.Attributes[0].Value,
					Amount: v.Attributes[1].Value,
					Action: v.Attributes[2].Value,
				}
				migratedEvents = append(migratedEvents, migratedAccount)
			}
		}
	}
	return mergedEvents, migratedEvents
}

func insertIntoDB(ctx context.Context, db *sql.DB, migratedAccounts []dblib.MigratedAccount, mergedAccount []dblib.MergedAccount) error {
	//Create a transaction on the database
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Insert data into merge_table Table
	stmt1, err := dblib.PrepareInsertMergeAccountQuery(ctx, tx)
	if err != nil {
		return fmt.Errorf("error preparing statement for Table1: %v", err)
	}
	defer stmt1.Close()

	// Insert data into migrated_account table
	stmt2, err := dblib.PrepareInsertMigratedAccountQuery(ctx, tx)
	if err != nil {
		return fmt.Errorf("error preparing statement for Table2: %v", err)
	}
	defer stmt2.Close()

	for _, d := range mergedAccount {
		err := dblib.ExecContextMergedAccount(ctx, stmt1, d)
		if err != nil {
			return fmt.Errorf("error inserting data into Table1: %v", err)
		}
	}

	for _, d := range migratedAccounts {
		err := dblib.ExecContextMigratedAccount(ctx, stmt2, d)
		if err != nil {
			return fmt.Errorf("error inserting data into Table1: %v", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}
