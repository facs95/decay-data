package db

type MergedAccount struct {
	ID                int
	Recipient         string
	ClaimedCoins      string
	FundCommunityPool string
	Height            string
}

type MigratedAccount struct {
	ID     int
	Sender string
	Action string
	Amount string
	Height string
}

type Error struct {
	ID        int
	Height    string
	EventType string
    TxIndex string
    EventIndex string
}
