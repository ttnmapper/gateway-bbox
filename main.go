package main

import (
	"flag"
	"github.com/streadway/amqp"
	"github.com/tkanos/gonfig"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"log"
	"sync"
	"ttnmapper-gw-bbox/types"
	"ttnmapper-gw-bbox/utils"
)

type Configuration struct {
	AmqpHost     string `env:"AMQP_HOST"`
	AmqpPort     string `env:"AMQP_PORT"`
	AmqpUser     string `env:"AMQP_USER"`
	AmqpPassword string `env:"AMQP_PASSWORD"`
	AmqpExchange string `env:"AMQP_EXCHANGE_INSERTED"`
	AmqpQueue    string `env:"AMQP_QUEUE"`

	PostgresHost     string `env:"POSTGRES_HOST"`
	PostgresPort     string `env:"POSTGRES_PORT"`
	PostgresUser     string `env:"POSTGRES_USER"`
	PostgresPassword string `env:"POSTGRES_PASSWORD"`
	PostgresDatabase string `env:"POSTGRES_DATABASE"`
	PostgresDebugLog bool   `env:"POSTGRES_DEBUG_LOG"`

	PrometheusPort string `env:"PROMETHEUS_PORT"`
}

var myConfiguration = Configuration{
	AmqpHost:     "localhost",
	AmqpPort:     "5672",
	AmqpUser:     "user",
	AmqpPassword: "password",
	AmqpExchange: "inserted_data",
	AmqpQueue:    "inserted_data_gateway_bbox",

	PostgresHost:     "localhost",
	PostgresPort:     "5432",
	PostgresUser:     "username",
	PostgresPassword: "password",
	PostgresDatabase: "database",
	PostgresDebugLog: false,

	PrometheusPort: "9100",
}

var db *gorm.DB
var rawPacketsChannel = make(chan amqp.Delivery)
var gatewayBboxCache sync.Map

func main() {
	reprocess := flag.Bool("reprocess", false, "a bool")
	flag.Parse()
	reprocess_gateways := flag.Args()

	err := gonfig.GetConf("conf.json", &myConfiguration)
	if err != nil {
		log.Println(err)
	}

	log.Printf("[Configuration]\n%s\n", utils.PrettyPrint(myConfiguration)) // output: [UserA, UserB]

	var gormLogLevel = logger.Silent
	if myConfiguration.PostgresDebugLog {
		log.Println("Database debug logging enabled")
		gormLogLevel = logger.Info
	}

	dsn := "host=" + myConfiguration.PostgresHost + " port=" + myConfiguration.PostgresPort + " user=" + myConfiguration.PostgresUser + " dbname=" + myConfiguration.PostgresDatabase + " password=" + myConfiguration.PostgresPassword + " sslmode=disable"
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(gormLogLevel),
	})
	if err != nil {
		panic(err.Error())
	}

	// Create tables if they do not exist
	log.Println("Performing auto migrate")
	if err := db.AutoMigrate(
		&types.GatewayBoundingBox{},
	); err != nil {
		log.Println("Unable autoMigrateDB - " + err.Error())
	}

	// Should we reprocess or listen for live data?
	if *reprocess {
		log.Println("Reprocessing")

		if len(reprocess_gateways) > 0 {
			ReprocessGateways(reprocess_gateways)
		} else {
			ReprocessAll()
		}

	} else {
		// Start amqp listener on this thread - blocking function
		log.Println("Starting AMQP thread")
		go subscribeToRabbitRaw()
		go processMessages()

		log.Printf("Init Complete")
		forever := make(chan bool)
		<-forever
	}
}

func ReprocessAll() {
	log.Println("All gateways")

	// Get all records
	var gateways []types.Gateway
	db.Find(&gateways)

	for _, gateway := range gateways {
		ReprocessSingleGateway(gateway.NetworkId, gateway.GatewayId)
	}
}

func ReprocessGateways(gateways []string) {
	for _, gatewayId := range gateways {
		// The same gateway_id can exist in multiple networks, so iterate them all
		var gateways []types.Gateway
		db.Where("gateway_id = ?", gatewayId).Find(&gateways)

		for _, gateway := range gateways {
			log.Println(gateway.NetworkId, " - ", gateway.GatewayId)
			ReprocessSingleGateway(gateway.NetworkId, gateway.GatewayId)
		}
	}
}

func ReprocessSingleGateway(networkId string, gatewayId string) {
	var antennas []types.Antenna
	db.Where("network_id = ? and gateway_id = ?", networkId, gatewayId).Find(&antennas)

	var antennaIds []uint
	for _, antenna := range antennas {
		antennaIds = append(antennaIds, antenna.ID)
	}

	var result types.GatewayBoundingBox
	db.Raw("SELECT max(latitude) as latitude_maximum, min(latitude) as latitude_minimum FROM packets WHERE antenna_id IN ? and latitude != 0", antennaIds).Scan(&result)
	db.Raw("SELECT max(longitude) as longitude_maximum, min(longitude) as longitude_minimum FROM packets WHERE antenna_id IN ? and longitude != 0", antennaIds).Scan(&result)
	log.Println(result)

	result.NetworkId = networkId
	result.GatewayId = gatewayId

	db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&result)
}
