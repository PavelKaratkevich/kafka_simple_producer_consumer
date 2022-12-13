package main

import (
	domain "kafka-go-getting-started/Domain"
	producer "kafka-go-getting-started/Producer"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()
	router.POST("/sendMsg", PostNewMsg)
	router.Run(":8000")
}

func PostNewMsg(ctx *gin.Context) {
	var msg domain.Message

	err := ctx.ShouldBindJSON(&msg)
	if err != nil {
		log.Printf("Error: %v", err.Error())
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"Message": "The message has been successfully sent to Kafka topic"})

	producer.SendMsgToKafka(msg)
}
