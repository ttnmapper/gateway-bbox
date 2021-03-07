package main

import (
	"encoding/json"
	"log"
	"time"
	"ttnmapper-gw-bbox/types"
	"ttnmapper-gw-bbox/utils"
)

func processMessages() {
	// Wait for a message and insert it into Postgres
	for d := range rawPacketsChannel {

		// The message form amqp is a json string. Unmarshal to ttnmapper uplink struct
		var message types.TtnMapperUplinkMessage
		if err := json.Unmarshal(d.Body, &message); err != nil {
			log.Print("AMQP " + err.Error())
			continue
		}

		// Iterate gateways. We store it flat in the database
		for _, gateway := range message.Gateways {
			updateTime := time.Unix(0, message.Time)
			log.Print("AMQP ", "", "\t", gateway.GatewayId+"\t", updateTime)
			gateway.Time = message.Time

			// Ignore locations obtained from live data
			gateway.Latitude = 0
			gateway.Longitude = 0
			gateway.Altitude = 0

			// Packet broker metadata will provide network id. For now assume TTN
			//gateway.NetworkId = "thethingsnetwork.org"

			updateGateway(message, gateway)
		}
	}
}

func updateGateway(message types.TtnMapperUplinkMessage, gateway types.TtnMapperGateway) {
	// Find the database IDs for this gateway and it's antennas
	gatewayDbBbox, err := getGatewayBboxDb(gateway)
	if err != nil {
		utils.FailOnError(err, "Can't find bbox in DB")
	}

	var boundsChanged = false
	// Latitude
	if gatewayDbBbox.LatitudeMaximum < message.Latitude {
		boundsChanged = true
		gatewayDbBbox.LatitudeMaximum = message.Latitude
	}
	if gatewayDbBbox.LatitudeMinimum < message.Latitude {
		boundsChanged = true
		gatewayDbBbox.LatitudeMinimum = message.Latitude
	}
	// Longitude
	if gatewayDbBbox.LongitudeMaximum < message.Longitude {
		boundsChanged = true
		gatewayDbBbox.LongitudeMaximum = message.Longitude
	}
	if gatewayDbBbox.LongitudeMinimum < message.Longitude {
		boundsChanged = true
		gatewayDbBbox.LongitudeMinimum = message.Longitude
	}

	if boundsChanged {
		db.Save(&gatewayDbBbox)
	}
}

func getGatewayBboxDb(gateway types.TtnMapperGateway) (types.GatewayBoundingBox, error) {

	gatewayIndexer := types.GatewayIndexer{NetworkId: gateway.NetworkId, GatewayId: gateway.GatewayId}
	i, ok := gatewayBboxCache.Load(gatewayIndexer)
	if ok {
		gatewayDb := i.(types.GatewayBoundingBox)
		return gatewayDb, nil

	} else {
		gatewayDb := types.GatewayBoundingBox{NetworkId: gateway.NetworkId, GatewayId: gateway.GatewayId}
		db.Where(&gatewayDb).First(&gatewayDb)
		if gatewayDb.ID == 0 {
			err := db.FirstOrCreate(&gatewayDb, &gatewayDb).Error
			if err != nil {
				return gatewayDb, err
			}
		}

		gatewayBboxCache.Store(gatewayIndexer, gatewayDb)
		return gatewayDb, nil
	}
}
