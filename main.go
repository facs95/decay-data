package main

import (
	"context"
	"database/sql"
	"fmt"
	dblib "github.com/facs95/decay-data/db"
	"github.com/facs95/decay-data/query"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

const (
	FromBlock  = 265401 // Block from which to start querying
	ToBlock    = 353670 // Last block to query
	BatchSize  = 10     // Amount of blocks per thread
	MaxWorkers = 5      // Amount of threads
)

func main() {
	// Set up database connection
	db, err := sql.Open("sqlite3", "./accounts.db")
	if err != nil {
		log.Fatalf("error opening database connection: %v", err)
	}
	defer db.Close()

	// Create en databases
	dblib.CreateMergeAccountTable(db)
	dblib.CreateMigrateAccountTable(db)
	dblib.CreateErrorTable(db)

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Process the range in batches
	if err := handleWorkers(ctx, db, FromBlock, ToBlock, BatchSize, MaxWorkers); err != nil {
		log.Fatalf("error processing range: %v", err)
	}
}

func handleWorkers(ctx context.Context, db *sql.DB, start, end, batchSize int, maxWorkers int) error {
	// Create a log file to have persistent logs
	logFile, err := os.OpenFile("./output.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer logFile.Close()

	wrt := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(wrt)

	// Create a channel to hold jobs to be executed by workers
	jobs := make(chan []int, maxWorkers)
	// Create a WaitGroup to wait for all workers to complete
	wg := sync.WaitGroup{}

	// Launch worker goroutines
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for job := range jobs {
				log.Printf("starting worker %v with blocks %v-%v", i, job[0], job[1])
				// Query the external resource for data
				mergedAccounts, migratedAccounts := processBatchOfBlocks(ctx, db, job)

				// Process the data and insert into MySQL database
				if err := insertIntoDB(ctx, db, migratedAccounts, mergedAccounts); err != nil {
					log.Printf("error inserting into database: %v", err)
					continue
				}
			}
		}(i)
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

func processBatchOfBlocks(ctx context.Context, db *sql.DB, job []int) ([]dblib.MergedEvents, []dblib.ClaimEvents) {
	mergedEvents, migratedEvents := []dblib.MergedEvents{}, []dblib.ClaimEvents{}
	for j := job[0]; j <= job[1]; j++ {
		height := strconv.Itoa(j)
		blockResult, err := query.GetBlockResult(height, 0)
		if err != nil {
			// This should be on Error database
			error := dblib.Error{
				Height: height,
			}
			if err := insertErrIntoDB(ctx, db, error); err != nil {
				log.Printf("error inserting error value on DB at height %v: %v", height, err)
			}
			log.Printf("error querying external resource at height %v: %v", height, err)
			continue
		}
		merged, migrated := filterAndDecodeEvents(blockResult.Result.TxsResults, j)
		mergedEvents = append(mergedEvents, merged...)
		migratedEvents = append(migratedEvents, migrated...)
	}
	log.Printf("finished job for blocks: %v - %v", job[0], job[1])
	return mergedEvents, migratedEvents
}

func filterAndDecodeEvents(txs []query.ResponseDeliverTx, height int) ([]dblib.MergedEvents, []dblib.ClaimEvents) {
	mergedEvents, migratedEvents := []dblib.MergedEvents{}, []dblib.ClaimEvents{}
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
					log.Printf("error decoding resource at height %v: %v", height, err)
					// we should add this records to error table
					// return nil, nil
					continue
				}
				mergeRecord := dblib.MergedEvents{
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
					log.Printf("error decoding resource at height %v: %v", height, err)
					// we should add this records to error table
					// return nil, nil
					continue
				}
				migratedAccount := dblib.ClaimEvents{
					Height: strconv.Itoa(height),
					Sender: v.Attributes[0].Value,
					Amount: v.Attributes[1].Value,
					Action: v.Attributes[2].Value,
				}

				// Decission was made to collect all claim data within decay block range
				// instead of only merged / migrated accounts
				// for context https://evmos.slack.com/archives/C022BMJSPQV/p1676632098959959
				migratedEvents = append(migratedEvents, migratedAccount)
			}
		}
	}
	return mergedEvents, migratedEvents
}

func insertErrIntoDB(ctx context.Context, db *sql.DB, error dblib.Error) error {
	//Create a transaction on the database
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Insert data into merge_table Table
	stmt1, err := dblib.PrepareInsertErrorQuery(ctx, tx)
	if err != nil {
		return fmt.Errorf("error preparing statement for ErrorTable: %v", err)
	}
	defer stmt1.Close()

	err = dblib.ExecContextError(ctx, stmt1, error)
	if err != nil {
		return fmt.Errorf("error inserting data into Table1: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

func insertIntoDB(ctx context.Context, db *sql.DB, migratedAccounts []dblib.ClaimEvents, mergedAccount []dblib.MergedEvents) error {
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
