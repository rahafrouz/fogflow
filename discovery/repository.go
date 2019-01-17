package main

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"

	. "github.com/smartfog/fogflow/common/config"
	. "github.com/smartfog/fogflow/common/ngsi"
)

type DBQuery struct {
	statement *sql.Stmt
	vars      []interface{}
}

func (q *DBQuery) Execute() {
	_, err := q.statement.Exec(q.vars...)
	if err != nil {
		ERROR.Println(err)
	}
}

type EntityRepository struct {
	// connection to the backend database
	db *sql.DB

	// cache all received registration in the memory for the performance reason
	ctxRegistrationList      map[string]*ContextRegistration
	ctxRegistrationList_lock sync.RWMutex

	// lock to control the update of database
	dbLock sync.RWMutex
}

func (er *EntityRepository) Init(config *DatabaseCfg) {
	var dbExist = false

	// initialize the registration list
	er.ctxRegistrationList = make(map[string]*ContextRegistration)

	for {
		exist, err := checkDatabase(config)

		if err == nil {
			dbExist = exist
			break
		} else {
			ERROR.Println("having some problem to connect to postgresql ", err)
			time.Sleep(2 * time.Second)
		}
	}

	//create the database if not exist
	if dbExist == false {
		createDatabase(config)
	} else {
		if config.DBReset == true {
			resetDatabase(config)
			createDatabase(config)
		}
	}

	//open the database
	er.db = openDatabase(config)

	INFO.Println("connected to postgresql")
}

func (er *EntityRepository) Close() {
	//close the database
	er.db.Close()
	INFO.Println("close the connection to postgresql")
}

//
// update the registration in the repository and also
// return a flag to indicate if there is anything in the repository before
//
func (er *EntityRepository) updateEntity(entity EntityId, registration *ContextRegistration) *ContextRegistration {
	updatedRegistration := er.updateRegistrationInMemory(entity, registration)

	// update the registration in the database as a background procedure
	go er.updateRegistrationInDataBase(entity, registration)

	// return the latest view of the registration for this entity
	return updatedRegistration
}

//
// for the performance purpose, we still keep the latest view of all registrations
//
func (er *EntityRepository) updateRegistrationInMemory(entity EntityId, registration *ContextRegistration) *ContextRegistration {
	er.ctxRegistrationList_lock.Lock()
	defer er.ctxRegistrationList_lock.Unlock()

	eid := entity.ID

	if existRegistration, exist := er.ctxRegistrationList[eid]; exist {
		// if the registration already exists, update it with the received update

		// update attribute table
		for _, attr := range registration.ContextRegistrationAttributes {
			for i, existAttr := range existRegistration.ContextRegistrationAttributes {
				if existAttr.Name == attr.Name {
					// remove the old one
					existRegistration.ContextRegistrationAttributes = append(existRegistration.ContextRegistrationAttributes[:i], existRegistration.ContextRegistrationAttributes[i+1:]...)
					break
				}
			}
			// append the new one
			existRegistration.ContextRegistrationAttributes = append(existRegistration.ContextRegistrationAttributes, attr)
		}

		// update metadata table
		for _, meta := range registration.Metadata {
			for i, existMeta := range existRegistration.Metadata {
				if existMeta.Name == meta.Name {
					// remove the old one
					existRegistration.Metadata = append(existRegistration.Metadata[:i], existRegistration.Metadata[i+1:]...)
					break
				}
			}
			// append the new one
			existRegistration.Metadata = append(existRegistration.Metadata, meta)
		}

		// update the provided URL
		if len(registration.ProvidingApplication) > 0 {
			existRegistration.ProvidingApplication = registration.ProvidingApplication
		}
	} else {
		er.ctxRegistrationList[eid] = registration
	}

	return er.ctxRegistrationList[eid]
}

//
// update the registration in the repository and also
// return a flag to indicate if there is anything in the repository before
//
func (er *EntityRepository) updateRegistrationInDataBase(entity EntityId, registration *ContextRegistration) error {
	er.dbLock.Lock()
	defer er.dbLock.Unlock()

	DEBUG.Println("UPDATE ENTITY-BEGIN")
	DEBUG.Println(entity.ID)

	queries := make([]DBQuery, 0)

	// update the entity table
	queryStatement := "SELECT entity_tab.eid, entity_tab.type, entity_tab.providerurl FROM entity_tab WHERE eid = $1;"
	rows, err := er.db.Query(queryStatement, entity.ID)
	if err != nil {
		return err
	}
	if rows.Next() == false {
		// insert new entity
		stmt, _ := er.db.Prepare("INSERT INTO entity_tab(eid, type, isPattern, providerURL) VALUES($1, $2, $3, $4);")
		query := DBQuery{statement: stmt, vars: []interface{}{entity.ID, entity.Type, entity.IsPattern, registration.ProvidingApplication}}
		queries = append(queries, query)
	}
	rows.Close()

	// update attribute table
	for _, attr := range registration.ContextRegistrationAttributes {
		queryStatement := "SELECT * FROM attr_tab WHERE attr_tab.eid = $1 AND attr_tab.name = $2;"
		rows, err := er.db.Query(queryStatement, entity.ID, attr.Name)
		if err == nil {
			if rows.Next() == false {
				// insert as new attribute
				stmt, _ := er.db.Prepare(`INSERT INTO attr_tab(eid, name, type, isDomain) VALUES($1, $2, $3, $4);`)
				query := DBQuery{statement: stmt, vars: []interface{}{entity.ID, attr.Name, attr.Type, attr.IsDomain}}
				queries = append(queries, query)
			} else {
				// update as existing attribute
				stmt, _ := er.db.Prepare("UPDATE attr_tab SET type = $1, isDomain = $2 WHERE attr_tab.eid = $3 AND attr_tab.name = $4;")
				query := DBQuery{statement: stmt, vars: []interface{}{attr.Type, attr.IsDomain, entity.ID, attr.Name}}
				queries = append(queries, query)
			}
		}
		rows.Close()
	}

	// update metadata table
	for _, meta := range registration.Metadata {
		switch strings.ToLower(meta.Type) {
		case "circle":
			circle := meta.Value.(Circle)
			queryStatement := "SELECT * FROM geo_circle_tab WHERE geo_circle_tab.eid = $1 AND geo_circle_tab.name = $2;"
			rows, err := er.db.Query(queryStatement, entity.ID, meta.Name)
			if err == nil {
				if rows.Next() == false {
					// insert as new attribute
					stmt, _ := er.db.Prepare("INSERT INTO geo_circle_tab(eid, name, type, center, radius) VALUES ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326), $6);")
					query := DBQuery{statement: stmt, vars: []interface{}{entity.ID, meta.Name, meta.Type, circle.Longitude, circle.Latitude, circle.Radius}}
					queries = append(queries, query)
				} else {
					// update as existing attribute                                        ,
					stmt, err := er.db.Prepare("UPDATE geo_circle_tab SET center = ST_SetSRID(ST_MakePoint($1, $2), 4326) AND radius = $3 WHERE geo_circle_tab.eid = $4 AND geo_circle_tab.name = $5;")
					if err != nil {
						ERROR.Println(err)
						return err
					}
					query := DBQuery{statement: stmt, vars: []interface{}{circle.Longitude, circle.Latitude, circle.Radius, entity.ID, meta.Name}}
					queries = append(queries, query)
				}
			}
			rows.Close()

		case "point":
			point := meta.Value.(Point)
			queryStatement := "SELECT * FROM geo_box_tab WHERE geo_box_tab.eid = $1 AND geo_box_tab.name = $2;"
			rows, err := er.db.Query(queryStatement, entity.ID, meta.Name)
			if err == nil {
				if rows.Next() == false {
					// insert as new attribute
					stmt, err := er.db.Prepare("INSERT INTO geo_box_tab(eid, name, type, box) VALUES ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326));")
					if err != nil {
						ERROR.Println(err)
						return err
					}
					query := DBQuery{statement: stmt, vars: []interface{}{entity.ID, meta.Name, meta.Type, point.Longitude, point.Latitude}}
					queries = append(queries, query)
				} else {
					// update as existing attribute
					stmt, err := er.db.Prepare("UPDATE geo_box_tab SET box = ST_SetSRID(ST_MakePoint($1, $2), 4326) WHERE geo_box_tab.eid = $3 AND geo_box_tab.name = $4;")
					if err != nil {
						ERROR.Println(err)
						return err
					}
					query := DBQuery{statement: stmt, vars: []interface{}{point.Longitude, point.Latitude, entity.ID, meta.Name}}
					queries = append(queries, query)
				}
			}
			rows.Close()

		case "polygon":
			polygon := meta.Value.(Polygon)
			locationText := ""
			for k, point := range polygon.Vertices {
				if k > 0 {
					locationText = locationText + ", "
				}
				locationText = locationText + fmt.Sprintf("%f %f", point.Longitude, point.Latitude)
			}

			queryStatement := "SELECT * FROM geo_box_tab WHERE geo_box_tab.eid = $1 AND geo_box_tab.name = $2;"
			rows, err := er.db.Query(queryStatement, entity.ID, meta.Name)
			if err == nil {
				if rows.Next() == false {
					// insert as new attribute
					stmt, err := er.db.Prepare("INSERT INTO geo_box_tab(eid, name, type, box) VALUES ($1, $2, $3, ST_MakePolygon(ST_GeomFromText('POLYGON(($4))', 4326)));")
					if err != nil {
						ERROR.Println(err)
						return err
					}
					query := DBQuery{statement: stmt, vars: []interface{}{entity.ID, meta.Name, meta.Type, locationText}}
					queries = append(queries, query)
				} else {
					// update as existing attribute
					stmt, err := er.db.Prepare("UPDATE geo_box_tab SET box = ST_MakePolygon(ST_GeomFromText('POLYGON(($1))', 4326)) WHERE geo_box_tab.eid = $2 AND geo_box_tab.name = $3;")
					if err != nil {
						ERROR.Println(err)
						return err
					}
					query := DBQuery{statement: stmt, vars: []interface{}{locationText, entity.ID, meta.Name}}
					queries = append(queries, query)
				}
			}
			rows.Close()

		default:
			queryStatement := "SELECT * FROM metadata_tab WHERE metadata_tab.eid = $1 AND metadata_tab.name = $2;"
			rows, err := er.db.Query(queryStatement, entity.ID, meta.Name)
			if err == nil {
				if rows.Next() == false {
					// insert as new attribute
					stmt, err := er.db.Prepare("INSERT INTO metadata_tab(eid, name, type, value) VALUES($1, $2, $3, $4);")
					if err != nil {
						ERROR.Println(err)
						return err
					}
					query := DBQuery{statement: stmt, vars: []interface{}{entity.ID, meta.Name, meta.Type, meta.Value}}
					queries = append(queries, query)
				} else {
					// update as existing attribute
					stmt, err := er.db.Prepare("UPDATE metadata_tab SET type = $1, value = $2 WHERE metadata_tab.eid = $3 AND metadata_tab.name = $4;")
					if err != nil {
						ERROR.Println(err)
						return err
					}
					query := DBQuery{statement: stmt, vars: []interface{}{meta.Type, meta.Value, entity.ID, meta.Name}}
					queries = append(queries, query)
				}
			}
			rows.Close()
		}
	}

	// apply the update once for the entire registration request, within a transaction
	er.execDBQuery(queries)

	DEBUG.Println("UPDATE ENTITY-END")
	DEBUG.Println(entity.ID)

	return nil
}

//
// query always goes to the database, just in order to take advantage of geoquery
//
func (er *EntityRepository) queryEntities(entities []EntityId, attributes []string, restriction Restriction) map[string][]EntityId {
	er.dbLock.RLock()
	defer er.dbLock.RUnlock()

	DEBUG.Println("QUERY ENTITY-BEGIN")

	entityMap := make(map[string][]EntityId)

	for _, entity := range entities {
		// three steps to construct the SQL statement to query the result
		queryStatement := "SELECT entity_tab.eid, entity_tab.type, entity_tab.ispattern, entity_tab.providerurl FROM entity_tab "

		// (1) consider attribute list
		for i, attr := range attributes {
			queryStatement = queryStatement + fmt.Sprintf(" INNER JOIN attr_tab at%d  ON entity_tab.eid = at%d.eid AND at%d.name = '%s' ",
				i+1, i+1, i+1, attr)
		}

		// (2) apply scopes to metadata
		boxTabFilter := ""
		orderBy := ""
		var num_of_geo_scopes int
		num_of_geo_scopes = 0
		for _, scope := range restriction.Scopes {
			switch strings.ToLower(scope.Type) {
			case "nearby":
				nearby := scope.Value.(NearBy)
				orderBy = fmt.Sprintf("  ST_Distance(geo_box_tab.box, ST_SetSRID(ST_MakePoint(%f, %f), 4326)) LIMIT %d ",
					nearby.Longitude, nearby.Latitude, nearby.Limit)

			case "circle":
				circle := scope.Value.(Circle)
				if num_of_geo_scopes == 0 {
					boxTabFilter = boxTabFilter + "( "
				} else {
					boxTabFilter = boxTabFilter + " OR "
				}
				boxTabFilter = boxTabFilter + fmt.Sprintf(" ST_DWithin(geo_box_tab.box, ST_SetSRID(ST_MakePoint(%f, %f), 4326), %f, true) ",
					circle.Longitude, circle.Latitude, circle.Radius)
				num_of_geo_scopes = num_of_geo_scopes + 1

			case "simplegeolocation":
				value := scope.Value.(Segment)
				segment := value.Converter()
				locationText := ""
				locationText = locationText + fmt.Sprintf("%f %f,", segment.NW_Corner.Longitude, segment.NW_Corner.Latitude)
				locationText = locationText + fmt.Sprintf("%f %f,", segment.NW_Corner.Longitude, segment.SE_Corner.Latitude)
				locationText = locationText + fmt.Sprintf("%f %f,", segment.SE_Corner.Longitude, segment.SE_Corner.Latitude)
				locationText = locationText + fmt.Sprintf("%f %f,", segment.SE_Corner.Longitude, segment.NW_Corner.Latitude)
				locationText = locationText + fmt.Sprintf("%f %f", segment.NW_Corner.Longitude, segment.NW_Corner.Latitude)

				if num_of_geo_scopes == 0 {
					boxTabFilter = boxTabFilter + "( "
				} else {
					boxTabFilter = boxTabFilter + " OR "
				}
				boxTabFilter = boxTabFilter + fmt.Sprintf(" ST_Within(geo_box_tab.box, ST_GeomFromText('POLYGON((%s))', 4326)) ",
					locationText)
				num_of_geo_scopes = num_of_geo_scopes + 1

			case "polygon":
				polygon := scope.Value.(Polygon)
				locationText := ""
				for k, point := range polygon.Vertices {
					if k > 0 {
						locationText = locationText + ", "
					}
					locationText = locationText + fmt.Sprintf("%f %f", point.Longitude, point.Latitude)
				}
				if num_of_geo_scopes == 0 {
					boxTabFilter = boxTabFilter + "( "
				} else {
					boxTabFilter = boxTabFilter + " OR "
				}
				boxTabFilter = boxTabFilter + fmt.Sprintf(" ST_Within(geo_box_tab.box, ST_GeomFromText('POLYGON((%s))', 4326)) ",
					locationText)
				num_of_geo_scopes = num_of_geo_scopes + 1

			case "stringquery":
				queryString := scope.Value.(string)
				constraints := strings.Split(queryString, ";")
				for i, constraint := range constraints {
					items := strings.Split(constraint, "=")
					queryStatement = queryStatement + fmt.Sprintf(" INNER JOIN metadata_tab md%d ON entity_tab.eid = md%d.eid and md%d.name = '%s' and md%d.value = '%s' ",
						i+1, i+1, i+1, items[0], i+1, items[1])
				}
			}
		}

		// (3) apply geo-scopes
		if boxTabFilter != "" {
			queryStatement = queryStatement + fmt.Sprintf(" INNER JOIN geo_box_tab ON entity_tab.eid = geo_box_tab.eid and %s) ", boxTabFilter)
		} else if orderBy != "" {
			queryStatement = queryStatement + fmt.Sprintf(" INNER JOIN geo_box_tab ON entity_tab.eid = geo_box_tab.eid ")
		}

		// (4) consider entity_id
		if entity.IsPattern == true {
			if entity.Type != "" && entity.ID != "" {
				queryStatement = queryStatement + fmt.Sprintf(" WHERE entity_tab.eid like '%s' AND entity_tab.type like '%s'",
					strings.Replace(entity.ID, ".*", "%", -1), strings.Replace(entity.Type, ".*", "%", -1))
			} else if entity.Type != "" {
				queryStatement = queryStatement + fmt.Sprintf(" WHERE entity_tab.type like '%s'",
					strings.Replace(entity.Type, ".*", "%", -1))
			} else if entity.ID != "" {
				queryStatement = queryStatement + fmt.Sprintf(" WHERE entity_tab.eid like '%s' ",
					strings.Replace(entity.ID, ".*", "%", -1))
			}
		} else {
			queryStatement = queryStatement + fmt.Sprintf(" WHERE entity_tab.eid = '%s' ", entity.ID)
		}

		// (5) consider sorting based on geo-distance
		if orderBy != "" {
			queryStatement = queryStatement + fmt.Sprintf(" ORDER BY %s ", orderBy)
		}

		DEBUG.Println(queryStatement)

		// perform the query
		rows, err := er.query(queryStatement)
		if err != nil {
			return nil
		}

		// prepare the result according the returned dataset
		for rows.Next() {
			var eid, etype, ispattern, providerURL string
			rows.Scan(&eid, &etype, &ispattern, &providerURL)

			var bIsPattern bool
			if ispattern == "true" {
				bIsPattern = true
			} else {
				bIsPattern = false
			}
			e := EntityId{ID: eid, Type: etype, IsPattern: bIsPattern}
			entityMap[providerURL] = append(entityMap[providerURL], e)
		}
		rows.Close()
	}

	DEBUG.Println("QUERY ENTITY-END")

	return entityMap
}

func (er *EntityRepository) deleteEntity(eid string) {
	er.dbLock.Lock()
	defer er.dbLock.Unlock()

	DEBUG.Println("DELETE ENTITY-BEGIN")
	DEBUG.Println(eid)

	// find out the associated entity
	queryStatement := "SELECT entity_tab.eid, entity_tab.type, entity_tab.providerurl FROM entity_tab WHERE eid = $1;"
	rows, err := er.db.Query(queryStatement, eid)
	if err != nil {
		return
	}

	queries := make([]DBQuery, 0)

	for rows.Next() {
		var entityID, entityType, providerURL string
		rows.Scan(&entityID, &entityType, &providerURL)

		if entityType == "IoTBroker" {
			DEBUG.Println("IoT Broker left as a context provider")
			er.ProviderLeft(providerURL)
		}

		// remove all attributes related to this entity
		stmt, _ := er.db.Prepare("DELETE FROM attr_tab WHERE eid = $1;")
		query := DBQuery{statement: stmt, vars: []interface{}{entityID}}
		queries = append(queries, query)

		// remove all metadata related to this entity
		stmt, _ = er.db.Prepare("DELETE FROM metadata_tab WHERE eid = $1;")
		query = DBQuery{statement: stmt, vars: []interface{}{entityID}}
		queries = append(queries, query)

		// remove all geo-metadata related to this entity
		stmt, _ = er.db.Prepare("DELETE FROM geo_box_tab WHERE eid = $1;")
		query = DBQuery{statement: stmt, vars: []interface{}{entityID}}
		queries = append(queries, query)

		stmt, _ = er.db.Prepare("DELETE FROM geo_circle_tab WHERE eid = $1;")
		query = DBQuery{statement: stmt, vars: []interface{}{entityID}}
		queries = append(queries, query)
	}
	rows.Close()

	// remove the entity
	stmt, _ := er.db.Prepare("DELETE FROM entity_tab WHERE eid =  $1;")
	query := DBQuery{statement: stmt, vars: []interface{}{eid}}
	queries = append(queries, query)

	er.execDBQuery(queries)

	DEBUG.Println("DELETE ENTITY-BEGIN")
	DEBUG.Println(eid)
}

func (er *EntityRepository) ProviderLeft(providerURL string) {
	// find out all entities associated with this broker
	queryStatement := "SELECT entity_tab.eid FROM entity_tab WHERE providerurl = $1;"
	rows, err := er.db.Query(queryStatement, providerURL)
	if err != nil {
		return
	}

	queries := make([]DBQuery, 0)

	for rows.Next() {
		var entityID string
		rows.Scan(&entityID)

		// remove all attributes related to this entity
		stmt, _ := er.db.Prepare("DELETE FROM attr_tab WHERE eid = $1;")
		query := DBQuery{statement: stmt, vars: []interface{}{entityID}}
		queries = append(queries, query)

		// remove all metadata related to this entity
		stmt, _ = er.db.Prepare("DELETE FROM metadata_tab WHERE eid = $1;")
		query = DBQuery{statement: stmt, vars: []interface{}{entityID}}
		queries = append(queries, query)

		// remove all geo-metadata related to this entity
		stmt, _ = er.db.Prepare("DELETE FROM geo_box_tab WHERE eid = $1;")
		query = DBQuery{statement: stmt, vars: []interface{}{entityID}}
		queries = append(queries, query)

		stmt, _ = er.db.Prepare("DELETE FROM geo_circle_tab WHERE eid = $1;")
		query = DBQuery{statement: stmt, vars: []interface{}{entityID}}
		queries = append(queries, query)
	}
	rows.Close()

	// remove all entities related to this registration
	stmt, _ := er.db.Prepare("DELETE FROM entity_tab WHERE providerurl = $1;")
	query := DBQuery{statement: stmt, vars: []interface{}{providerURL}}
	queries = append(queries, query)

	er.execDBQuery(queries)
}

func (er *EntityRepository) retrieveRegistration(entityID string) *ContextRegistration {
	er.dbLock.RLock()
	defer er.dbLock.RUnlock()

	return er.ctxRegistrationList[entityID]
}

/*  disable this due to the performance reason
func (er *EntityRepository) retrieveRegistration(entityID string) *ContextRegistration {
	er.dbLock.RLock()
	defer er.dbLock.RUnlock()

	DEBUG.Println("RETRIEVE ENTITY-BEGIN")
	DEBUG.Println(entityID)

	// query all entities associated with this registrationId
	queryStatement := "SELECT eid, type, isPattern, providerURL FROM entity_tab WHERE entity_tab.eid = $1;"
	rows, err := er.db.Query(queryStatement, entityID)
	if err != nil {
		ERROR.Println(err)
		DEBUG.Println("RETRIEVE ENTITY-END")
		DEBUG.Println(entityID)
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var eid, etype, epattern, providerURL string
		rows.Scan(&eid, &etype, &epattern, &providerURL)

		ctxRegistration := ContextRegistration{}

		entities := make([]EntityId, 0)

		entity := EntityId{}
		entity.ID = eid
		entity.Type = etype

		if epattern == "true" {
			entity.IsPattern = true
		} else {
			entity.IsPattern = false
		}

		DEBUG.Println("========test=======")
		DEBUG.Println(entity)

		entities = append(entities, entity)

		ctxRegistration.EntityIdList = entities
		ctxRegistration.ProvidingApplication = providerURL

		// query all attributes that belong to those entities
		registeredAttributes := make([]ContextRegistrationAttribute, 0)

		queryStatement := "SELECT name, type, isDomain FROM attr_tab WHERE attr_tab.eid = $1;"
		results, err := er.db.Query(queryStatement, eid)
		if err != nil {
			ERROR.Println(err)
			DEBUG.Println("RETRIEVE ENTITY-END")
			DEBUG.Println(entityID)
			return nil
		}
		for results.Next() {
			var name, attributeType, isDomain string
			results.Scan(&name, &attributeType, &isDomain)

			attr := ContextRegistrationAttribute{}
			attr.Name = name
			attr.Type = attributeType

			if isDomain == "true" {
				attr.IsDomain = true
			} else {
				attr.IsDomain = false
			}

			registeredAttributes = append(registeredAttributes, attr)
		}
		results.Close()

		DEBUG.Println("========test==2=====")
		DEBUG.Println(registeredAttributes)

		ctxRegistration.ContextRegistrationAttributes = registeredAttributes

		// query all metadatas that belong to those entities
		registeredMetadatas := make([]ContextMetadata, 0)

		queryStatement = "SELECT name, type, value FROM metadata_tab WHERE metadata_tab.eid = $1;"
		results, err = er.db.Query(queryStatement, eid)
		if err != nil {
			ERROR.Println(err)
			DEBUG.Println("RETRIEVE ENTITY-END")
			DEBUG.Println(entityID)
			return nil
		}
		for results.Next() {
			var name, mdType, value string
			results.Scan(&name, &mdType, &value)

			metadata := ContextMetadata{}
			metadata.Name = name
			metadata.Type = mdType
			metadata.Value = value

			registeredMetadatas = append(registeredMetadatas, metadata)
		}
		results.Close()

		DEBUG.Println("========test==3=====")
		DEBUG.Println(registeredMetadatas)

		// query all geo-related metadatas that belong to those entities
		queryStatement = "SELECT name, type, ST_AsText(box) FROM geo_box_tab WHERE geo_box_tab.eid = $1;"
		results, err = er.db.Query(queryStatement, eid)
		if err != nil {
			ERROR.Println(err)
			DEBUG.Println("RETRIEVE ENTITY-END")
			DEBUG.Println(entityID)
			return nil
		}
		for results.Next() {
			DEBUG.Println("-------he------")
			var name, mtype, box string
			results.Scan(&name, &mtype, &box)

			DEBUG.Println("-------check the retrieve from the database------")
			DEBUG.Printf("%s, %s, %+v \n", name, mtype, box)

			metadata := ContextMetadata{}
			metadata.Name = name
			metadata.Type = mtype

			switch mtype {
			case "point":
				var latitude, longitude float64
				DEBUG.Println("-------he-2-----")
				_, err := fmt.Scanf(box, "POINT(%f %f)", &longitude, &latitude)
				if err == nil {
					point := Point{}
					point.Latitude = latitude
					point.Longitude = longitude

					metadata.Value = point
				} else {
					ERROR.Println(err)
				}

			case "polygon":
				metadata.Value = box
			}

			registeredMetadatas = append(registeredMetadatas, metadata)
		}
		results.Close()

		DEBUG.Println("========test==4=====")
		DEBUG.Println(registeredMetadatas)

		queryStatement = "SELECT name, ST_AsText(center), radius FROM geo_circle_tab WHERE geo_circle_tab.eid = $1;"
		results, err = er.db.Query(queryStatement, eid)
		if err != nil {
			ERROR.Println(err)
			DEBUG.Println("RETRIEVE ENTITY-END")
			DEBUG.Println(entityID)
			return nil
		}
		for results.Next() {
			var name, mtype, center string
			var radius float64
			results.Scan(&name, &center, &radius)

			metadata := ContextMetadata{}
			metadata.Name = name
			metadata.Type = mtype

			circle := Circle{}

			var latitude, longitude float64
			_, err := fmt.Scanf(center, "POINT(%f %f)", &longitude, &latitude)
			if err == nil {
				circle.Latitude = latitude
				circle.Longitude = longitude
				circle.Radius = radius

				metadata.Value = circle
			}

			registeredMetadatas = append(registeredMetadatas, metadata)
		}
		results.Close()

		DEBUG.Println("========test==5=====")
		DEBUG.Println(registeredMetadatas)

		ctxRegistration.Metadata = registeredMetadatas

		DEBUG.Println("RETRIEVE ENTITY-END")
		DEBUG.Println(entityID)
		DEBUG.Printf("%+v\n", ctxRegistration)

		return &ctxRegistration
	}

	DEBUG.Println("RETRIEVE ENTITY-END")
	DEBUG.Println(entityID)

	return nil
}

*/

func (er *EntityRepository) query(statement string) (*sql.Rows, error) {
	return er.db.Query(statement)
}

func (er *EntityRepository) execDBQuery(queries []DBQuery) {
	DEBUG.Println("===========SQL============")
	for _, query := range queries {
		query.Execute()
	}
}
