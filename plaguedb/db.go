package plaguedb

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/hashicorp/golang-lru/v2/expirable"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/log/v3"
	_ "github.com/lib/pq"
	"os"
	"strings"
	"time"
)

type PlagueWatcher struct {
	db    *sql.DB
	cache *expirable.LRU[string, string]
}

type PreparedTransaction struct {
	peerId      string
	txHash      string
	txFee       string
	gasFeeCap   string
	gasTipCap   string
	txFirstSeen int64
	receiver    string
	signer      string
	nonce       string
}

type TxSummaryTransaction struct {
	peerId      string
	txHash      string
	txFirstSeen int64
}

func Init() (*PlagueWatcher, error) {
	db, err := OpenDB()
	if err != nil {
		return nil, err
	}
	cache := expirable.NewLRU[string, string](1000000, nil, time.Hour*24)
	return &PlagueWatcher{db: db, cache: cache}, nil
}

func (pw *PlagueWatcher) HandleTxs(txs types2.TxSlots, ctx *types2.TxParseContext, peerId []byte) error {
	preparedTxs, txsSummary := pw.prepareTransactions(txs, ctx, peerId)
	if len(preparedTxs) == 0 && len(txsSummary) == 0 {
		log.Warn("No new txs")
		return nil
	}

	if len(preparedTxs) == 0 && len(txsSummary) > 0 {
		log.Warn("Storing txs summary", "txs", len(txsSummary))
		pw.StoreTxSummary(txsSummary)
	}

	// TODO: will be the 2nd step
	//if len(preparedTxs) > 0 {
	//	log.Warn("Storing txs", "txs", len(preparedTxs))
	//	pw.StoreTxs(preparedTxs, txsSummary)
	//}

	return nil
}

func (pw *PlagueWatcher) prepareTransactions(txs types2.TxSlots, ctx *types2.TxParseContext, peerId []byte) ([]*PreparedTransaction, []*TxSummaryTransaction) {
	// empty slice of prepared transactions
	var preparedTxs []*PreparedTransaction
	var txSummary []*TxSummaryTransaction
	for _, tx := range txs.Txs {
		// check if tx is already in cache
		var txHash = string(tx.IDHash[:])
		if _, ok := pw.cache.Get(txHash); ok {
			continue
		}

		// append this transaction to final list of TxSummary
		ts := time.Now().UnixMilli()
		txSummary = append(txSummary, &TxSummaryTransaction{
			peerId:      hex.EncodeToString(peerId),
			txHash:      "0x" + hex.EncodeToString(tx.IDHash[:]),
			txFirstSeen: ts,
		})

		// TODO: will be the 2nd step
		// TODO: sender pubkey -> hex.EncodeToString(txs.Senders.At(i)[:])
		//gasFeeCap := tx.GasFeeCapUint().Clone().String()
		//gasTipCap := tx.GasTipCapUint().String()
		//fee := strconv.FormatUint(tx.GasPrice().Uint64()*tx.Gas(), 10)
		//nonce := strconv.FormatUint(tx.Nonce(), 10)
		//signer := types.NewLondonSigner(tx.ChainId())
		//addr, err := signer.Sender(tx)
		//if err != nil {
		//	log.Warn("Failed to get the sender:", "err", err)
		//	addr = common.HexToAddress("0x438308")
		//}
		//var to string
		//if tx.To() == nil {
		//	to = "0x0"
		//} else {
		//	to = tx.To().Hex()
		//}
		//preparedTxs = append(preparedTxs, &PreparedTransaction{
		//	txHash:      tx.Hash().Hex(),
		//	txFee:       fee,
		//	gasFeeCap:   gasFeeCap,
		//	gasTipCap:   gasTipCap,
		//	txFirstSeen: ts,
		//	receiver:    to,
		//	signer:      addr.Hex(),
		//	nonce:       nonce,
		//})

		// memorize that we've retrieved this transaction
		pw.cache.Add(txHash, txHash)
	}
	///Summary
	///tx_hash string, peer_id string, tx_first_seen int64

	///Fetched
	///tx_hash string, tx_fee string, gas_fee_cap string, gas_tip_cap string, tx_first_seen int64, from string, to string, nonce string

	return preparedTxs, txSummary
}

func (pw *PlagueWatcher) StoreTxSummary(txs []*TxSummaryTransaction) {
	sqlstring := `WITH input_rows(tx_hash, peer_id, tx_first_seen) AS (
		VALUES %s
	)
	INSERT INTO tx_summary_erigon (tx_hash, peer_id, tx_first_seen)
	SELECT input_rows.tx_hash, input_rows.peer_id, input_rows.tx_first_seen
	FROM input_rows
	ON CONFLICT (tx_hash, peer_id) DO NOTHING;`
	valuesSQL := ""
	for _, tx := range txs {
		valuesSQL += fmt.Sprintf("('%s', '%s', %d),", tx.txHash, tx.peerId, tx.txFirstSeen)
	}
	valuesSQL = strings.TrimSuffix(valuesSQL, ",")
	query := fmt.Sprintf(sqlstring, valuesSQL)
	_, err := pw.db.Exec(query)
	if err != nil {
		log.Warn("Failed to insert txs:", "err", err)
	}
}

func (pw *PlagueWatcher) StoreTxs(txs []*PreparedTransaction, txsSummary []*TxSummaryTransaction) {
	sqlstring := `WITH input_rows(tx_hash, tx_fee, gas_fee_cap, gas_tip_cap, tx_first_seen, receiver, signer, nonce, peer_id) AS (
		VALUES %s),
	input_rows2(tx_hash, peer_id, tx_first_seen) AS (
		VALUES %s
	), ins AS (
		INSERT INTO tx_fetched_erigon (tx_hash, tx_fee, gas_fee_cap, gas_tip_cap, tx_first_seen, receiver, signer, nonce)
		SELECT input_rows.tx_hash, input_rows.tx_fee, input_rows.gas_fee_cap, input_rows.gas_tip_cap, input_rows.tx_first_seen, input_rows.receiver, input_rows.signer, input_rows.nonce FROM input_rows
		ON CONFLICT (tx_hash) DO NOTHING
		RETURNING id, tx_hash
	 )
	 INSERT INTO tx_summary_erigon (tx_hash, peer_id, tx_first_seen)
	 SELECT tx_hash, peer_id, tx_first_seen
	 FROM input_rows2
	 ON CONFLICT (tx_hash, peer_id) DO NOTHING;`
	valuesSQL := ""
	valuesSQL2 := ""

	for _, tx := range txs {
		valuesSQL += fmt.Sprintf("('%s', '%s', '%s', '%s', %d, '%s', '%s', %s, '%s'),", tx.txHash, tx.txFee, tx.gasFeeCap, tx.gasTipCap, tx.txFirstSeen, tx.receiver, tx.signer, tx.nonce, tx.peerId)
	}
	for _, tx := range txsSummary {
		valuesSQL2 += fmt.Sprintf("('%s', '%s', %d),", tx.txHash, tx.peerId, tx.txFirstSeen)
	}
	valuesSQL = strings.TrimSuffix(valuesSQL, ",")
	valuesSQL2 = strings.TrimSuffix(valuesSQL2, ",")
	query := fmt.Sprintf(sqlstring, valuesSQL, valuesSQL2)
	_, err := pw.db.Exec(query)
	if err != nil {
		log.Warn("Failed to insert txs:", "err", err)
	}
}

func OpenDB() (*sql.DB, error) {
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	log.Warn("Opening DB")

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Warn("Failed to open DB", "err", err)
		return nil, err
	}

	log.Warn("DB opened")
	return db, nil
}

//func (pw *PlagueWatcher) HandleBlocksFetched(block *types.Block, peer string, peerRemoteAddr string, peerLocalAddr string) error {
//	ts := time.Now().UnixMilli()
//	insertSQL := `INSERT INTO block_fetched_erigon(block_hash, block_number, first_seen_ts, peer, peer_remote_addr, peer_local_addr) VALUES($1,$2,$3,$4,$5,$6)`
//	log.Warn("Inserting block", "block", block.NumberU64())
//	_, err := pw.db.Exec(insertSQL, block.Hash().Hex(), block.NumberU64(), ts, peer, peerRemoteAddr, peerLocalAddr)
//	return err
//}

//
//func (pw *PlagueWatcher) HandleTxsOld(txs []*types.Transaction, peerID string) error {
//	// log.Warn("Handling txs", "peerID", peerID, "txs", len(txs))
//	preparedTxs, txs_summary := pw.prepareTransactions(txs)
//	if len(preparedTxs) == 0 && len(txs_summary) == 0 {
//		log.Warn("No new txs")
//		return nil
//	}
//	if len(preparedTxs) > 0 {
//		log.Warn("Storing txs", "txs", len(preparedTxs))
//		pw.StoreTxs(preparedTxs, txs_summary)
//	}
//	if len(preparedTxs) == 0 && len(txs_summary) > 0 {
//		log.Warn("Storing txs summary", "txs", len(txs_summary))
//		pw.StoreTxSummary(txs_summary, peerID)
//	}
//	return nil
//}
//
//func (pw *PlagueWatcher) prepareTransactionsOld(txs []*types.Transaction) ([]*PreparedTransaction, []*TxSummaryTransaction) {
//	//empty slice of prepared transactions
//	var preparedTxs []*PreparedTransaction
//	var tx_summary []*TxSummaryTransaction
//	for _, tx := range txs {
//		//check if tx is already in cache
//		ts := time.Now().UnixMilli()
//		tx_summary = append(tx_summary, &TxSummaryTransaction{
//			txHash:      tx.Hash().Hex(),
//			txFirstSeen: ts,
//		})
//		if _, ok := pw.cache.Get(tx.Hash().Hex()); ok {
//			continue
//		}
//		gasFeeCap := tx.GasFeeCapUint().Clone().String()
//		gasTipCap := tx.GasTipCapUint().String()
//		fee := strconv.FormatUint(tx.GasPrice().Uint64()*tx.Gas(), 10)
//		nonce := strconv.FormatUint(tx.Nonce(), 10)
//		signer := types.NewLondonSigner(tx.ChainId())
//		addr, err := signer.Sender(tx)
//		if err != nil {
//			log.Warn("Failed to get the sender:", "err", err)
//			addr = common.HexToAddress("0x438308")
//		}
//		var to string
//		if tx.To() == nil {
//			to = "0x0"
//		} else {
//			to = tx.To().Hex()
//		}
//		preparedTxs = append(preparedTxs, &PreparedTransaction{
//			txHash:      tx.Hash().Hex(),
//			txFee:       fee,
//			gasFeeCap:   gasFeeCap,
//			gasTipCap:   gasTipCap,
//			txFirstSeen: ts,
//			receiver:    to,
//			signer:      addr.Hex(),
//			nonce:       nonce,
//		})
//		pw.cache.Add(tx.Hash().Hex(), tx.Hash().Hex())
//	}
//	///Summary
//	///tx_hash string, peer_id string, tx_first_seen int64
//
//	///Fetched
//	///tx_hash string, tx_fee string, gas_fee_cap string, gas_tip_cap string, tx_first_seen int64, from string, to string, nonce string
//
//	return preparedTxs, tx_summary
//}

func prepareAndExecQuery(db *sql.DB, queryString string) error {
	query, err := db.Prepare(queryString)
	if err != nil {
		return err
	}
	_, err = query.Exec()
	return err
}
