package db

import (
	"database/sql"
	"fmt"

	"golang.org/x/net/context"
)

func CreateErrorTable(db *sql.DB) {
	sqlStmt := `
	   create table if not exists error (
	    id integer not null primary key,
	    height text,
        event_type text
        tx_index text
        event_index text
	);`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		fmt.Printf("Error executing the table creation: %q", err)
		panic("Stop processing")
	}
}

func CreateMergedEventTable(db *sql.DB) {
	sqlStmt := `
	   create table if not exists merged_event (
	    id integer not null primary key,
	    recipient text,
        sender text,
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

func CreateClaimEventTable(db *sql.DB) {
	sqlStmt := `
	   create table if not exists claim_event (
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

func PrepareInsertErrorQuery(ctx context.Context, tx *sql.Tx) (*sql.Stmt, error) {
	insertError, err := tx.PrepareContext(ctx, "insert into error(height, event_type, tx_index, event_index) values(?,?,?,?)")
	if err != nil {
		fmt.Printf("Error preparing transaction: %q", err)
		return nil, err
	}
	return insertError, nil
}

func PrepareInsertMergeEventQuery(ctx context.Context, tx *sql.Tx) (*sql.Stmt, error) {
	insertAccount, err := tx.PrepareContext(ctx, "insert into merged_event(recipient, height, claimed_coins, fund_community_pool_coins) values(?,?,?,?)")
	if err != nil {
		fmt.Printf("Error preparing transaction: %q", err)
		return nil, err
	}
	return insertAccount, nil
}

func PrepareUpdateSenderMergeEventQuery(ctx context.Context, tx *sql.Tx) (*sql.Stmt, error) {
	updateSender, err := tx.PrepareContext(ctx, "UPDATE merged_event SET sender =  ? WHERE id = ?")
	if err != nil {
		fmt.Printf("Error preparing transaction: %q", err)
		return nil, err
	}
	return updateSender, nil
}

func ExecContextMergeEventUpdate(ctx context.Context, stmt *sql.Stmt, event MergedEvent) error {
	_, err := stmt.ExecContext(ctx, event.Sender, event.ID)
	if err != nil {
		return fmt.Errorf("error inserting data into MergedAccount: %v", err)
	}
	return nil
}

func ExecContextError(ctx context.Context, stmt *sql.Stmt, error Error) error {
	// Insert data into Error
	_, err := stmt.ExecContext(ctx, error.Height, error.EventType, error.TxIndex, error.EventIndex)
	if err != nil {
		return fmt.Errorf("error inserting data into MergedAccount: %v", err)
	}
	return nil
}

func ExecContextMergedEvent(ctx context.Context, stmt *sql.Stmt, account MergedEvent) error {
	// Insert data into Table1
	_, err := stmt.ExecContext(ctx, account.Recipient, account.Height, account.ClaimedCoins, account.FundCommunityPool)
	if err != nil {
		return fmt.Errorf("error inserting data into MergedAccount: %v", err)
	}
	return nil
}

func PrepareInsertClaimEventQuery(ctx context.Context, tx *sql.Tx) (*sql.Stmt, error) {
	insertAccount, err := tx.PrepareContext(ctx, "insert into claim_event(sender, height, amount, claim_action) values(?,?,?,?)")
	if err != nil {
		fmt.Printf("Error preparing transaction: %q", err)
		return nil, err
	}
	return insertAccount, nil
}

func ExecContextClaimEvent(ctx context.Context, stmt *sql.Stmt, account ClaimEvent) error {
	// Insert data into Table1
	_, err := stmt.ExecContext(ctx, account.Sender, account.Height, account.Amount, account.Action)
	if err != nil {
		return fmt.Errorf("error inserting data into MigratedAccount: %v", err)
	}
	return nil
}
