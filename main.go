package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric-chaincode-go/shim"
	sc "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/sirupsen/logrus"
)

type SmartContract struct{}

type MatchedTrans struct {
	ID            string    `json:"id"`
	DocType       string    `json:"doctype"`
	Transdatetime time.Time `json:"transdatetime"`
	Uetr          string    `json:"uetr"`
}

type UPF struct {
	ID            string    `json:"id"`
	DocType       string    `json:"doctype"`
	RecordType    string    `json:"recordtype"`
	AccountNumber string    `json:"accountnumber"`
	TodaysDate    string    `json:"todaysdate"`
	Amount        string    `json:"amount"`
	EntrySign     string    `json:"entrysign"`
	TransDateTime time.Time `json:"transdatetime"`
	Reference1    string    `json:"ref1"`
	UetrId        string    `json:"uetr"`
	Reference2    string    `json:"ref2"`
	Reference4    string    `json:"ref4"`
	Status        string    `json:"status"`
	ReasonCode    string    `json:"reason"`
	Reconciled    bool      `json:"reconcile"`
	CreatedDt     string    `json:"crtdate"`
	UpdateDt      string    `json:"upddt"`
}

type GL struct {
	ID            string    `json:"id"`
	DocType       string    `json:"doctype"`
	Recon         string    `json:"recon"`
	RecordType    string    `json:"recordtype"`
	Account       string    `json:"account"`
	Currency      string    `json:"currency"`
	TransDateTime time.Time `json:"transdatetime"`
	Amount        float64   `json:"amount"`
	Entrysign     string    `json:"entrysign"`
	Date1         string    `json:"Date1"`
	Date2         string    `json:"Date2"`
	UetrId        string    `json:"uetr"`
	Branch        string    `json:"branch"`
	Reconciled    bool      `json:"reconcile"`
	CreatedDt     string    `json:"crtdate"`
	UpdateDt      string    `json:"upddt"`
}

type MarkOffAcc struct {
	ID                string    `json:"id"`
	DocType           string    `json:"doctype"`
	UetrId            string    `json:"uetr"`
	UetrId30          string    `json:"uetr30"`
	InstructBIC       string    `json:"instructbic"`
	CreditorAgentBIC  string    `json:"creditoragentbic"`
	Amount            float64   `json:"amount"`
	TransDateTime     time.Time `json:"transdatetime"`
	SettlementPoolRef string    `json:"settlementpoolref"`
	Reconciled        bool      `json:"reconcile"`
	Status            string    `json:"status"`
}

func (s *SmartContract) Init(APIstub shim.ChaincodeStubInterface) sc.Response {
	return shim.Success(nil)
}

func (s *SmartContract) Invoke(APIstub shim.ChaincodeStubInterface) sc.Response {

	logrus.Println("Starting ...... ")
	function, args := APIstub.GetFunctionAndParameters()

	logrus.Printf(" args.....XXXX %v", args)

	switch function {
	case "loadTrans":
		return s.loadTransaction(APIstub, args)
	case "queryTrans":
		return s.queryTransaction(APIstub, args)
	case "queryUnmatchTrans":
		return s.queryUnmatchTrans(APIstub, args)
	case "Reconciliation":
		return s.Reconciliation(APIstub, args)
	case "UpdateTrans":
		return s.updateTrans(APIstub, args)
	case "ReconciliationRecon2":
		return s.ReconciliationRecon2(APIstub, args)
	case "ReconciliationRecon3":
		return s.ReconciliationRecon3(APIstub, args)
	default:
		return shim.Error("Invalid Smart Contract function name.")
	}
}

func (s *SmartContract) loadTransaction(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {
	if len(args) != 1 {
		return shim.Error("Incorrect number of arguments... Expecting 2")
	}

	var data map[string]interface{}
	err := json.Unmarshal([]byte(args[0]), &data)

	if err != nil {
		return shim.Error("invalid JSON format: " + err.Error())
	}

	if args[0] == "" {
		return shim.Error("Transaction ID cannot be empty")
	}

	err = validateTransaction(data)
	if err != nil {
		return shim.Error(fmt.Sprintf("Validation failed: %s", err))
	}
	//layout := time.RFC3339
	//data["transdatetime"], _ = time.Parse(layout, data["transdatetime"].(string))

	transactionAsBytes, err := json.Marshal(data)
	if err != nil {
		return shim.Error(fmt.Sprintf("Failed to marshal transaction: %s", err))
	}
	id := data["id"].(string)
	err = APIstub.PutState(id, transactionAsBytes)
	if err != nil {
		return shim.Error(fmt.Sprintf("Failed to create transaction: %s", err))
	}

	logrus.Printf("Transaction created: %s", args[0])
	return shim.Success(nil)
}

func validateTransaction(transaction map[string]interface{}) error {

	if transaction["doctype"].(string) == "" {
		return fmt.Errorf("doctype cannot be empty")
	}
	if transaction["id"].(string) == "" {
		return fmt.Errorf("id cannot be empty")
	}

	switch transaction["doctype"].(string) {
	case "CARD", "CASA", "UPF":
		if transaction["uetr"].(string) == "" {
			return fmt.Errorf("Reference3/uetr cannot be empty")
		}

		if transaction["amount"].(string) == "" {
			return fmt.Errorf("amount cannot be empty")
		}
	case "MARKOFFREJ":
		if transaction["uetr"].(string) == "" {
			return fmt.Errorf("Reference3/uetr cannot be empty")
		}

		if transaction["amount"].(string) == "" {
			return fmt.Errorf("amount cannot be empty")
		}
		//default:
		//	return fmt.Errorf("No proper doctype")

	}

	return nil
}

func (s *SmartContract) queryTransaction(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {
	if len(args) != 1 {
		return shim.Error("Incorrect number of arguments. Expecting 1")
	}

	transactionAsBytes, err := APIstub.GetState(args[0])
	if err != nil {
		return shim.Error(fmt.Sprintf("Failed to get transaction: %s", err))
	} else if transactionAsBytes == nil {
		return shim.Error("Transaction not found")

	}

	logrus.Printf("Result %s", string(transactionAsBytes))
	return shim.Success(transactionAsBytes)
}

func (s *SmartContract) queryUnmatchTrans(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {
	startDate := args[0]
	endDate := args[1]

	doctypeArr := strings.Split(args[2], ",")

	if _, err := time.Parse(time.RFC3339, startDate); err != nil {
		return shim.Error(fmt.Sprintf("startDate must be in ISO 8601 format (RFC3339): %v", err))
	}
	if _, err := time.Parse(time.RFC3339, endDate); err != nil {
		return shim.Error(fmt.Sprintf("endDate must be in ISO 8601 format (RFC3339): %v", err))
	}

	var queryString strings.Builder

	queryString.WriteString(`{"selector":{"doctype":{"$in":[`)
	for i, value := range doctypeArr {
		if i > 0 {
			queryString.WriteString(",")
		}
		queryString.WriteString(fmt.Sprintf(`"%s"`, value))
	}
	queryString.WriteString(`]},"transdatetime":{"$gte":"`)
	queryString.WriteString(startDate)
	queryString.WriteString(`","$lte":"`)
	queryString.WriteString(endDate)
	queryString.WriteString(`"},"reconcile": false},"use_index": "indexOwnerDateStatus"}`)

	//queryString.WriteString(`","reconcile": false},"use_index": "indexOwnerDateStatus"}`)

	/*queryStrMaster := fmt.Sprintf(`{
	        "selector": {
	            "transdatetime": {
	                "$gte": "%s",
	                "$lte": "%s"
	            },
				"reconcile": false,
				"doctype":{"$in": %s}
	        },
			"use_index": "indexOwnerDateStatus"
	    }`, startDate, endDate, doctype)*/

	logrus.Printf("queryStrMaster.....queryStrMaster %v", queryString.String())

	resultsIterator, err := APIstub.GetQueryResult(queryString.String())
	if err != nil {
		return shim.Error(fmt.Sprintf("Return queryStrMaster: %v", err))
	}
	defer resultsIterator.Close()

	var queryReturn []map[string]interface{}

	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return shim.Error(fmt.Sprintf("Return queryStrMaster fetch next: %v", err))
		}

		var eachQuery map[string]interface{}
		err = json.Unmarshal(queryResponse.Value, &eachQuery)
		if err != nil {
			return shim.Error(fmt.Sprintf("Return queryStrMaster unmarshal: %v", err))
		}

		queryReturn = append(queryReturn, eachQuery)

	}

	transactionsAsBytes, err := json.Marshal(queryReturn)

	if err != nil {
		return shim.Error(fmt.Sprintf("Failed to marshal transactions: %s", err))
	}

	return shim.Success(transactionsAsBytes)
}

func (s *SmartContract) Reconciliation(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {
	startDate := args[0]
	endDate := args[1]

	if _, err := time.Parse(time.RFC3339, startDate); err != nil {
		return shim.Error(fmt.Sprintf("startDate must be in ISO 8601 format (RFC3339): %v", err))
	}
	if _, err := time.Parse(time.RFC3339, endDate); err != nil {
		return shim.Error(fmt.Sprintf("endDate must be in ISO 8601 format (RFC3339): %v", err))
	}

	queryStrMaster := fmt.Sprintf(`{
        "selector": {
            "transdatetime": {
                "$gte": "%s",
                "$lte": "%s"
            },
			"reconcile": false,
			"doctype":"UPF"
        },
		"use_index": "indexOwnerDateStatus"
    }`, startDate, endDate)

	logrus.Printf("queryStrMaster.....queryStrMaster %v", queryStrMaster)

	resultsIterator, err := APIstub.GetQueryResult(queryStrMaster)
	if err != nil {
		return shim.Error(fmt.Sprintf("Return queryStrMaster: %v", err))
	}
	defer resultsIterator.Close()

	var matchedTrans MatchedTrans

	var matchedTransAll []MatchedTrans

	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return shim.Error(fmt.Sprintf("Return queryStrMaster fetch next: %v", err))
		}

		var upf UPF
		err = json.Unmarshal(queryResponse.Value, &upf)
		if err != nil {
			return shim.Error(fmt.Sprintf("Return queryStrMaster unmarshal: %v", err))
		}

		logrus.Printf("UPF Record..... %v", upf)

		queryStrOTH := fmt.Sprintf(`{
        "selector": {
			"uetr": "%s",
			"reconcile": false,
			"status": "%s",
			"entrysign": "%s",
			"doctype":{"$in": ["CARD","CASA","MARKOFFREJ","MARKOFFACC"]}
        	},
			"use_index": "indexOwnerDateStatus"
    	}`, upf.UetrId, upf.Status, upf.EntrySign)

		logrus.Printf("queryStrOTH.....queryStrOTH %v", queryStrOTH)

		resultsIteratorOTH, err1 := APIstub.GetQueryResult(queryStrOTH)

		if err1 != nil {
			return shim.Error(fmt.Sprintf("Return Query queryStrOTH: %v", err1))
		}

		defer resultsIteratorOTH.Close()

		for resultsIteratorOTH.HasNext() { // should return one record

			queryResponseOTH, err := resultsIteratorOTH.Next()

			logrus.Printf("Matched.....resultsIteratorOTH %v", resultsIteratorOTH)

			if err != nil {
				return shim.Error(fmt.Sprintf("Return resultsIteratorOTH: %v", err))
			}

			var queryReturn map[string]interface{}
			err = json.Unmarshal(queryResponseOTH.Value, &queryReturn)
			if err != nil {
				return shim.Error(fmt.Sprintf("Return queryResponseOTH: %v", err))
			}

			matchedTrans.ID = upf.ID
			matchedTrans.DocType = upf.DocType
			matchedTrans.Transdatetime = upf.TransDateTime
			matchedTrans.Uetr = upf.UetrId

			matchedTransAll = append(matchedTransAll, matchedTrans)
			logrus.Printf("Matched Trans All1 %v", matchedTransAll)

			matchedTrans.ID = queryReturn["id"].(string)
			matchedTrans.DocType = queryReturn["doctype"].(string)
			matchedTrans.Uetr = queryReturn["uetr"].(string)
			var transdatetime = queryReturn["transdatetime"].(string)
			matchedTrans.Transdatetime, _ = time.Parse(time.RFC3339, transdatetime)
			matchedTransAll = append(matchedTransAll, matchedTrans)

			logrus.Printf("Matched Trans All2 %v", matchedTransAll)
		}

	}

	transactionsAsBytes, err := json.Marshal(matchedTransAll)

	if err != nil {
		return shim.Error(fmt.Sprintf("Failed to marshal transactions: %s", err))
	}

	return shim.Success(transactionsAsBytes)
}

func (s *SmartContract) ReconciliationRecon2(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {
	startDate := args[0]
	endDate := args[1]

	if _, err := time.Parse(time.RFC3339, startDate); err != nil {
		return shim.Error(fmt.Sprintf("startDate must be in ISO 8601 format (RFC3339): %v", err))
	}
	if _, err := time.Parse(time.RFC3339, endDate); err != nil {
		return shim.Error(fmt.Sprintf("endDate must be in ISO 8601 format (RFC3339): %v", err))
	}

	queryStrGL := fmt.Sprintf(`{
        "selector": {
            "transdatetime": {
                "$gte": "%s",
                "$lte": "%s"
            },
			"reconcile": false,
			"doctype":"GL"
        },
		"use_index": "indexOwnerDateStatus"
    }`, startDate, endDate)

	logrus.Printf("queryStrGL.....queryStrGL %v", queryStrGL)

	resultsIterator, err := APIstub.GetQueryResult(queryStrGL)
	if err != nil {
		return shim.Error(fmt.Sprintf("Return queryStrGL: %v", err))
	}
	defer resultsIterator.Close()

	var matchedTrans MatchedTrans

	var matchedTransAll []MatchedTrans

	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return shim.Error(fmt.Sprintf("Return queryStrGL fetch next: %v", err))
		}

		var GlTrans GL
		err = json.Unmarshal(queryResponse.Value, &GlTrans)
		if err != nil {
			return shim.Error(fmt.Sprintf("Return queryStrGL unmarshal: %v", err))
		}

		logrus.Printf("GlTrans Record..... %v", GlTrans)

		queryStrOTH := fmt.Sprintf(`{
        "selector": {
			"transdatetime": {
                "$gte": "%s",
                "$lte": "%s"
            },
			"uetr30": "%s",
			"reconcile": false,
			"amount": %f,
			"doctype": "MARKOFFACC"}
        	},
			"use_index": "indexOwnerDateStatus"
    	}`, startDate, endDate, GlTrans.UetrId, GlTrans.Amount)

		logrus.Printf("resultsIteratorMarkOff.....resultsIteratorMarkOff %v", queryStrOTH)

		resultsIteratorMarkOff, err1 := APIstub.GetQueryResult(queryStrOTH)

		if err1 != nil {
			return shim.Error(fmt.Sprintf("Return Query resultsIteratorMarkOff: %v", err1))
		}

		defer resultsIteratorMarkOff.Close()

		for resultsIteratorMarkOff.HasNext() { // should return one record

			queryResponseMarkOff, err := resultsIteratorMarkOff.Next()

			logrus.Printf("Matched.....resultsIteratorMarkOff %v", resultsIteratorMarkOff)

			if err != nil {
				return shim.Error(fmt.Sprintf("Return resultsIteratorOTH: %v", err))
			}

			var queryReturn map[string]interface{}
			err = json.Unmarshal(queryResponseMarkOff.Value, &queryReturn)
			if err != nil {
				return shim.Error(fmt.Sprintf("Return queryResponseMarkOff: %v", err))
			}

			matchedTrans.ID = GlTrans.ID
			matchedTrans.DocType = GlTrans.DocType
			matchedTrans.Transdatetime = GlTrans.TransDateTime
			matchedTrans.Uetr = GlTrans.UetrId

			matchedTransAll = append(matchedTransAll, matchedTrans)
			logrus.Printf("Matched Trans All1 %v", matchedTransAll)

			matchedTrans.ID = queryReturn["id"].(string)
			matchedTrans.DocType = queryReturn["doctype"].(string)
			matchedTrans.Uetr = queryReturn["uetr30"].(string)
			var transdatetime = queryReturn["transdatetime"].(string)
			matchedTrans.Transdatetime, _ = time.Parse(time.RFC3339, transdatetime)
			matchedTransAll = append(matchedTransAll, matchedTrans)

			logrus.Printf("Matched Trans All2 %v", matchedTransAll)
		}

	}

	transactionsAsBytes, err := json.Marshal(matchedTransAll)

	if err != nil {
		return shim.Error(fmt.Sprintf("Failed to marshal transactions: %s", err))
	}

	return shim.Success(transactionsAsBytes)
}

func (s *SmartContract) ReconciliationRecon3(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {
	startDate := args[0]
	endDate := args[1]

	if _, err := time.Parse(time.RFC3339, startDate); err != nil {
		return shim.Error(fmt.Sprintf("startDate must be in ISO 8601 format (RFC3339): %v", err))
	}
	if _, err := time.Parse(time.RFC3339, endDate); err != nil {
		return shim.Error(fmt.Sprintf("endDate must be in ISO 8601 format (RFC3339): %v", err))
	}

	//---------------------Summation of Amount matching ----------------------------------------------------------------
	querySettlementPool := fmt.Sprintf(`{
		"selector": {
			"transdatetime": {
				"$gte": "%s",
				"$lte": "%s"
			},
			"doctype": "MARKOFFACC"}
			},
			"fields": ["id", "settlementpoolref", "instructbic", "creditoragentbic","amount","transdatetime"],
			"use_index": "indexOwnerDateStatus"
		}`, startDate, endDate)

	settlePoolIterator, err := APIstub.GetQueryResult(querySettlementPool)
	if err != nil {
		return shim.Error(fmt.Sprintf("Return settlePoolIterator: %v", err))
	}
	defer settlePoolIterator.Close()

	var amount [7]float64
	matchmarkoffTrans := make([]map[string]interface{}, 7)

	for i := range matchmarkoffTrans {
		matchmarkoffTrans[i] = make(map[string]interface{})
	}

	for settlePoolIterator.HasNext() {
		queryResponse, err := settlePoolIterator.Next()
		if err != nil {
			return shim.Error(fmt.Sprintf("Return querySettlementPool fetch next: %v", err))
		}

		var markOffAccTrans MarkOffAcc
		err = json.Unmarshal(queryResponse.Value, &markOffAccTrans)
		if err != nil {
			return shim.Error(fmt.Sprintf("Return querySettlementPool unmarshal: %v", err))
		}

		i, _ := strconv.Atoi(markOffAccTrans.SettlementPoolRef)

		matchmarkoffTranseach := map[string]interface{}{
			"id":            markOffAccTrans.ID,
			"doctype":       markOffAccTrans.DocType,
			"transdatetime": markOffAccTrans.TransDateTime,
			"uetr":          markOffAccTrans.UetrId,
		}

		matchmarkoffTrans[i-1][markOffAccTrans.ID] = matchmarkoffTranseach

		if markOffAccTrans.InstructBIC == "NEDSZAJJ" {

			amount[i-1] = amount[i-1] + markOffAccTrans.Amount
		} else {

			amount[i-1] = amount[i-1] - markOffAccTrans.Amount
		}

	}

	logrus.Printf("matchmarkoffTranseach.... %v", matchmarkoffTrans)

	queryStrGLSum := fmt.Sprintf(`{
	        "selector": {
	            "transdatetime": {
	                "$gte": "%s",
	                "$lte": "%s"
	            },
				"uetr":"RPN",
				"reconcile": false,
				"doctype":"GL"
	        },
			"use_index": "indexOwnerDateStatus"
	    }`, startDate, endDate)

	resultsIteratorSum, err := APIstub.GetQueryResult(queryStrGLSum)
	if err != nil {
		return shim.Error(fmt.Sprintf("Return resultsIteratorSum: %v", err))
	}
	defer resultsIteratorSum.Close()

	var finalMarkoffAcc []interface{}

	for resultsIteratorSum.HasNext() {
		queryResponse, err := resultsIteratorSum.Next()
		if err != nil {
			return shim.Error(fmt.Sprintf("Return resultsIteratorSum fetch next: %v", err))
		}

		var GlTrans GL
		err = json.Unmarshal(queryResponse.Value, &GlTrans)
		if err != nil {
			return shim.Error(fmt.Sprintf("Return resultsIteratorSum unmarshal: %v", err))
		}

		chk, j := contains(amount, GlTrans.Amount)

		if chk {

			for _, each := range matchmarkoffTrans[j] {
				finalMarkoffAcc = append(finalMarkoffAcc, each)
			}

			finalMarkoffAcc = append(finalMarkoffAcc, map[string]interface{}{
				"id":            GlTrans.ID,
				"doctype":       GlTrans.DocType,
				"uetr":          GlTrans.UetrId,
				"transdatetime": GlTrans.TransDateTime,
			})

		}
	}

	//------------------------------------------------------------------------------------------------------------------

	logrus.Printf("finalMarkoffAcc finalMarkoffAcc.... %v", finalMarkoffAcc)

	transactionsAsBytes, err := json.Marshal(finalMarkoffAcc)

	if err != nil {
		return shim.Error(fmt.Sprintf("Failed to marshal transactions: %s", err))
	}

	return shim.Success(transactionsAsBytes)

}

func contains(arr [7]float64, target float64) (bool, int) {
	i := 0
	for _, value := range arr {
		if math.Abs(value) == target {
			return true, i
		}
		i++
	}
	return false, 0
}

func main() {

	logrus.SetFormatter(&logrus.JSONFormatter{})

	err := shim.Start(new(SmartContract))
	if err != nil {
		logrus.Fatalf("Error starting Smart Contract: %s", err)
	}
}

func (s *SmartContract) updateTrans(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {

	if len(args) != 2 {
		return shim.Error("Incorrect number of arguments. Expecting 2")
	}

	var DLTIds []string

	var DLTIDRet []string

	DLTIds = strings.Split(args[0], ",")

	logrus.Printf("DLTIds %v", DLTIds)

	for _, Id := range DLTIds {

		recAsBytes, err := APIstub.GetState(Id)
		if err != nil {
			return shim.Error(fmt.Sprintf("failed to read from world state: %v", err))
		}
		if recAsBytes == nil {
			return shim.Error(fmt.Sprintf("Transaction with ID %s does not exist", Id))
		}

		// Unmarshal the JSON data into a  struct
		var rec map[string]interface{}
		err = json.Unmarshal(recAsBytes, &rec)
		if err != nil {
			return shim.Error(fmt.Sprintf("failed to unmarshal JSON for  ID %s: %v", Id, err))
		}

		logrus.Printf("rec11 %v", rec)

		// Update the owner field
		rec["reconcile"] = true
		rec["upddt"] = string(args[1])

		logrus.Printf("rec %v", rec)

		// Marshal the updated struct back to JSON
		updatedRecAsBytes, err := json.Marshal(rec)
		if err != nil {
			return shim.Error(fmt.Sprintf("failed to marshal JSON for car ID %s: %v", Id, err))
		}

		// Write the updated asset back to the ledger
		err = APIstub.PutState(Id, updatedRecAsBytes)
		if err != nil {
			return shim.Error(fmt.Sprintf("failed to update car in world state for car ID %s: %v", Id, err))
		}

		DLTIDRet = append(DLTIDRet, Id)
	}

	updatedRecAsBytes, err := json.Marshal(DLTIDRet)

	if err != nil {
		return shim.Error(fmt.Sprintf("failed to marshal %v", err))
	}

	return shim.Success(updatedRecAsBytes)
}
