package db

type MergedEvent struct {
	ID                int
	Recipient         string
	Sender            string
	ClaimedCoins      string
	FundCommunityPool string
	Height            string
}

type ClaimEvent struct {
	ID     int
	Sender string
	Action string
	Amount string
	Height string
}

type Error struct {
	ID         int
	Height     string
	EventType  string
	TxIndex    string
	EventIndex string
}
