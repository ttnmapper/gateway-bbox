package main

import (
	"encoding/json"
	"github.com/patrickmn/go-cache"
	"log"
	"time"
	"ttnmapper-gw-bbox/types"
	"ttnmapper-gw-bbox/utils"
)

var (
	gatewayDbCache   *cache.Cache
	gatewayBboxCache *cache.Cache
)

func processMessages() {

	gatewayDbCache = cache.New(120*time.Minute, 10*time.Minute)
	gatewayBboxCache = cache.New(120*time.Minute, 10*time.Minute)

	// Wait for a message and insert it into Postgres
	for d := range rawPacketsChannel {

		// The message form amqp is a json string. Unmarshal to ttnmapper uplink struct
		var message types.TtnMapperUplinkMessage
		if err := json.Unmarshal(d.Body, &message); err != nil {
			log.Print("AMQP " + err.Error())
			continue
		}

		// Do not use experiment data for bounding box
		if message.Experiment != "" {
			continue
		}

		// Ignore messages without location
		if message.Latitude == 0 && message.Longitude == 0 {
			continue
		}

		// Iterate gateways. We store it flat in the database
		for _, gateway := range message.Gateways {
			updateTime := time.Unix(0, message.Time)
			log.Print(message.NetworkId, "\t", gateway.GatewayId, "\t", updateTime)

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
	if gatewayDbBbox.North == 0 || message.Latitude > gatewayDbBbox.North {
		boundsChanged = true
		gatewayDbBbox.North = message.Latitude
	}
	if gatewayDbBbox.South == 0 || message.Latitude < gatewayDbBbox.South {
		boundsChanged = true
		gatewayDbBbox.South = message.Latitude
	}
	// Longitude
	if gatewayDbBbox.East == 0 || message.Longitude > gatewayDbBbox.East {
		boundsChanged = true
		gatewayDbBbox.East = message.Longitude
	}
	if gatewayDbBbox.West == 0 || message.Longitude < gatewayDbBbox.West {
		boundsChanged = true
		gatewayDbBbox.West = message.Longitude
	}

	// Also take gateway location into account
	gatewayDb := GetGatewayDb(gateway)
	if gatewayDb.ID == 0 || (gatewayDb.Latitude == 0 && gatewayDb.Longitude == 0) {
		// Gateway not in DB, or location not set
	} else {
		if gatewayDbBbox.North == 0 || gatewayDb.Latitude > gatewayDbBbox.North {
			boundsChanged = true
			gatewayDbBbox.North = gatewayDb.Latitude
		}
		if gatewayDbBbox.South == 0 || gatewayDb.Latitude < gatewayDbBbox.South {
			boundsChanged = true
			gatewayDbBbox.South = gatewayDb.Latitude
		}
		if gatewayDbBbox.East == 0 || gatewayDb.Longitude > gatewayDbBbox.East {
			boundsChanged = true
			gatewayDbBbox.East = gatewayDb.Longitude
		}
		if gatewayDbBbox.West == 0 || gatewayDb.Longitude < gatewayDbBbox.West {
			boundsChanged = true
			gatewayDbBbox.West = gatewayDb.Longitude
		}
	}

	if boundsChanged {
		log.Println("Bounding box grew")
		db.Save(&gatewayDbBbox)
	}
}

func getGatewayBboxDb(gateway types.TtnMapperGateway) (types.GatewayBoundingBox, error) {
	gatewayKey := gateway.NetworkId + "/" + gateway.GatewayId

	// Try to load from cache first
	if x, found := gatewayBboxCache.Get(gatewayKey); found {
		//log.Println("  [d] Cache hit")
		gatewayBbox := x.(types.GatewayBoundingBox)
		return gatewayBbox, nil
	}

	gatewayBboxDb := types.GatewayBoundingBox{NetworkId: gateway.NetworkId, GatewayId: gateway.GatewayId}
	db.Where(&gatewayBboxDb).First(&gatewayBboxDb)
	if gatewayBboxDb.ID == 0 {
		log.Println("Gateway not found in database, creating")
		err := db.FirstOrCreate(&gatewayBboxDb, &gatewayBboxDb).Error
		if err != nil {
			return gatewayBboxDb, err
		}
	}

	gatewayBboxCache.Set(gatewayKey, gatewayBboxDb, cache.DefaultExpiration)
	return gatewayBboxDb, nil
}

func GetGatewayDb(gateway types.TtnMapperGateway) types.Gateway {
	gatewayKey := gateway.NetworkId + "/" + gateway.GatewayId

	// Try to load from cache first
	if x, found := gatewayDbCache.Get(gatewayKey); found {
		//log.Println("  [d] Cache hit")
		gatewayDb := x.(types.Gateway)
		return gatewayDb
	}

	var gatewayDb types.Gateway
	db.Where("network_id = ? and gateway_id = ?", gateway.NetworkId, gateway.GatewayId).First(&gatewayDb)

	gatewayDbCache.Set(gatewayKey, gatewayDb, cache.DefaultExpiration)
	return gatewayDb
}
