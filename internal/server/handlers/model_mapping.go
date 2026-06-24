package handlers

import (
	"net/http"
	"strconv"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/server/middleware"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/server/router"
	"github.com/gin-gonic/gin"
)

func init() {
	router.NewGroupRouter("/api/v1/model-mapping").
		Use(middleware.Auth()).
		AddRoute(router.NewRoute("/list", http.MethodGet).Handle(listModelMapping)).
		AddRoute(router.NewRoute("/create", http.MethodPost).Handle(createModelMapping)).
		AddRoute(router.NewRoute("/update", http.MethodPost).Handle(updateModelMapping)).
		AddRoute(router.NewRoute("/delete/:id", http.MethodDelete).Handle(deleteModelMapping))
}

func listModelMapping(c *gin.Context) {
	mappings, err := op.ModelMappingList(c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	resp.Success(c, mappings)
}

func createModelMapping(c *gin.Context) {
	var req model.ModelMappingCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.InvalidJSON(c)
		return
	}
	mapping, err := op.ModelMappingCreate(&req, c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	resp.Success(c, mapping)
}

func updateModelMapping(c *gin.Context) {
	var req model.ModelMappingUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.InvalidJSON(c)
		return
	}
	mapping, err := op.ModelMappingUpdate(&req, c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	resp.Success(c, mapping)
}

func deleteModelMapping(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil || id <= 0 {
		resp.Error(c, http.StatusBadRequest, "invalid id")
		return
	}
	if err := op.ModelMappingDelete(id, c.Request.Context()); err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	resp.Success(c, nil)
}
