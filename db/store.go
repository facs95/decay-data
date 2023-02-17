package db

import (
	"database/sql"
	"fmt"

	"golang.org/x/net/context"
)

func CreateErrorTable(db *sql.DB) {
	sqlStmt := `
	   create table if not exists error_event (
	    id integer not null primary key,
	    height text,
        event_type text
	);`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		fmt.Printf("Error executing the table creation: %q", err)
		panic("Stop processing")
	}
}

func CreateMergeAccountTable(db *sql.DB) {
	sqlStmt := `
	   create table if not exists merged_account (
	    id integer not null primary key,
	    recipient text,
        height text,
        claimed_coins text,
        fund_community_pool_coins text
	);`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		fmt.Printf("Error executing the table creation: %q", err)
		panic("Stop processing")
	}
}

func CreateMigrateAccountTable(db *sql.DB) {
	sqlStmt := `
	   create table if not exists migrated_account (
	    id integer not null primary key,
        sender text,
        height text,
        amount text,
        claim_action text
	);`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		fmt.Printf("Error executing the table creation: %q", err)
		panic("Stop processing")
	}
}

func PrepareInsertMergeAccountQuery(ctx context.Context, tx *sql.Tx) (*sql.Stmt, error) {
	insertAccount, err := tx.PrepareContext(ctx, "insert into merged_account(recipient, height, claimed_coins, fund_community_pool_coins) values(?,?,?,?)")
	if err != nil {
		fmt.Printf("Error preparing transaction: %q", err)
		return nil, err
	}
	return insertAccount, nil
}

func ExecContextMergedAccount(ctx context.Context, stmt *sql.Stmt, account MergedAccount) error {
	// Insert data into Table1
	_, err := stmt.ExecContext(ctx, account.Recipient, account.Height, account.ClaimedCoins, account.FundCommunityPool)
	if err != nil {
		return fmt.Errorf("error inserting data into MergedAccount: %v", err)
	}
	return nil
}

func PrepareInsertMigratedAccountQuery(ctx context.Context, tx *sql.Tx) (*sql.Stmt, error) {
	insertAccount, err := tx.PrepareContext(ctx, "insert into migrated_account(sender, height, amount, claim_action) values(?,?,?,?)")
	if err != nil {
		fmt.Printf("Error preparing transaction: %q", err)
		return nil, err
	}
	return insertAccount, nil
}

func ExecContextMigratedAccount(ctx context.Context, stmt *sql.Stmt, account MigratedAccount) error {
	// Insert data into Table1
	_, err := stmt.ExecContext(ctx, account.Sender, account.Height, account.Amount, account.Action)
	if err != nil {
		return fmt.Errorf("error inserting data into MigratedAccount: %v", err)
	}
	return nil
}
