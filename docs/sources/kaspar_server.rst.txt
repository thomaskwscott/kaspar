.. _kaspar_server:

Kaspar Server
=============

Kaspar Server provides a REST interface in which to establish table metadata and submit SQL.

:ref:`config options <kaspar_server_config>`

Building and Running
--------------------

Kaspar Server is a maven project so supports the usual goals.

.. sourcecode:: bash

    mvn clean package

API Reference
-------------

----------
Query APIs
----------

POST /query
###########

Receives a SQL statement and returns a queryId that can be used to track it.

Example Payload:

.. sourcecode:: bash

    SELECT * FROM Customers_json

Example Response:

.. sourcecode:: bash

    {"queryId":"cb3482ed_b1e5_4fd7_a863_5e94f2c55792"}

GET /query?gueryId={query id}
#############################

Get the status of a submitted query

Example Response:

.. sourcecode:: bash

    {"queryId":"cb3482ed_b1e5_4fd7_a863_5e94f2c55792","status":"COMPLETE"}

GET /query
##########

Get the status of all submitted queries

Example Response:

.. sourcecode:: bash

    {
	  "getQueryStatusResponses": [
        {
		  "queryId": "cb3482ed_b1e5_4fd7_a863_5e94f2c55792",
		  "status": "COMPLETE"
	    }
      ]
    }

-----------
Result APIs
-----------

GET /result?queryId={query id}
##############################

Retrieves results for the given completed query id

Example Response:

.. sourcecode:: bash

    {
	  "getRowResponses": [{
		"getColumnResponses": [{
			"name": "_offset",
			"value": "0"
		  }, {
			"name": "_partition",
			"value": "0"
		  }, {
			"name": "_timestamp",
			"value": "1635022725415"
		  }, {
			"name": "id",
			"value": "1"
		  }, {
			"name": "name",
			"value": "Harry Potter"
		  }, {
			"name": "address",
			"value": "4 Privet Drive"
		  }, {
			"name": "age",
			"value": "11"
		  }]
	  }, {
		"getColumnResponses": [{
			"name": "_offset",
			"value": "1"
		  }, {
			"name": "_partition",
			"value": "0"
		  }, {
			"name": "_timestamp",
			"value": "1635022725456"
		  }, {
			"name": "id",
			"value": "4"
		  }, {
			"name": "name",
			"value": "Buffy Summers"
		  }, {
			"name": "address",
			"value": "1630 Revello Drive"
		  }, {
			"name": "age",
			"value": "27"
		  }]
	  }, {
		"getColumnResponses": [{
			"name": "_offset",
			"value": "2"
	  	  }, {
			"name": "_partition",
			"value": "0"
		  }, {
			"name": "_timestamp",
			"value": "1635022725460"
          }, {
			"name": "id",
			"value": "7"
		  }, {
			"name": "name",
			"value": "Fox Mulder"
		  }, {
			"name": "address",
			"value": "2630 Hegal Place"
		  }, {
			"name": "age",
			"value": "35"
		  }]
	  }, {
		"getColumnResponses": [{
			"name": "_offset",
			"value": "0"
		  }, {
			"name": "_partition",
			"value": "2"
		  }, {
			"name": "_timestamp",
			"value": "1635022725416"
		  }, {
			"name": "id",
			"value": "2"
		  }, {
			"name": "name",
			"value": "Arthur Dent"
		  }, {
			"name": "address",
			"value": "155 Country Lane"
		  }, {
			"name": "age",
			"value": "42"
		  }]
	  }, {
		"getColumnResponses": [{
			"name": "_offset",
			"value": "1"
		  }, {
			"name": "_partition",
			"value": "2"
		  }, {
			"name": "_timestamp",
			"value": "1635022725457"
		  }, {
			"name": "id",
			"value": "5"
		  }, {
			"name": "name",
			"value": "Norm"
		  }, {
			"name": "address",
			"value": "84 Beacon Street"
		  }, {
			"name": "age",
			"value": "40"
		  }]
	  }, {
		"getColumnResponses": [{
			"name": "_offset",
			"value": "2"
		  }, {
			"name": "_partition",
			"value": "2"
		  }, {
			"name": "_timestamp",
			"value": "1635022725461"
		  }, {
			"name": "id",
			"value": "8"
		  }, {
			"name": "name",
			"value": "Charles Xavier"
		  }, {
			"name": "address",
			"value": "1407 Graymalkin Lane"
		  }, {
			"name": "age",
			"value": "80"
		  }]
	  }, {
		"getColumnResponses": [{
			"name": "_offset",
			"value": "0"
		  }, {
			"name": "_partition",
			"value": "4"
		  }, {
			"name": "_timestamp",
			"value": "1635022725375"
		  }, {
			"name": "id",
			"value": "0"
		  }, {
			"name": "name",
			"value": "Bruce Wayne"
		  }, {
			"name": "address",
			"value": "Wayne Manor"
		  }, {
			"name": "age",
			"value": "36"
		  }]
	  }, {
		"getColumnResponses": [{
			"name": "_offset",
			"value": "1"
		  }, {
			"name": "_partition",
			"value": "4"
		  }, {
			"name": "_timestamp",
			"value": "1635022725416"
		  }, {
			"name": "id",
			"value": "3"
		  }, {
			"name": "name",
			"value": "Clark Kent"
		  }, {
			"name": "address",
			"value": "344 Clinton Street, Apartment 3D"
		  }, {
			"name": "age",
			"value": "24"
		  }]
	  }, {
		"getColumnResponses": [{
			"name": "_offset",
			"value": "2"
		  }, {
			"name": "_partition",
			"value": "4"
		  }, {
			"name": "_timestamp",
			"value": "1635022725459"
		  }, {
			"name": "id",
			"value": "6"
		  }, {
			"name": "name",
			"value": "Spongebob Squarepants"
		  }, {
			"name": "address",
			"value": "124 Conch Street"
		  }, {
			"name": "age",
			"value": "34"
		  }]
	  }, {
		"getColumnResponses": [{
			"name": "_offset",
			"value": "3"
		  }, {
			"name": "_partition",
			"value": "4"
		  }, {
			"name": "_timestamp",
			"value": "1635022725462"
		  }, {
			"name": "id",
			"value": "9"
		  }, {
			"name": "name",
			"value": "Peter Parker"
		  }, {
			"name": "address",
			"value": "20 Ingram Street"
		  }, {
			"name": "age",
			"value": "17"
		  }]
	  }]
    }

GET /result
###########

List the results sets available

Example Response:

.. sourcecode:: bash

    {
	  "getResultResponses": [{
		"queryId": "cb3482ed_b1e5_4fd7_a863_5e94f2c55792"
	  }]
    }

DELETE /result?queryId={query id}
#################################

Delete a result set from persistent storage.

Example Response:

.. sourcecode:: bash

    Results Deleted

----------
Table APIs
----------

POST /table
###########

Creates a table in the metastore to be queried later.

Example Payload:

.. sourcecode:: bash

    {
      "tableName" : "Customers_json",
      "tableSpec" : {
        "deserializerClass": "kaspar.dataload.structure.PathJsonValueRowDeserializer",
        "config": {
          "columns" : [
            {
              "name": "id",
              "type": "Integer",
              "path": "$.id"
            },
            {
              "name": "name",
              "type": "String",
              "path": "$.name"
            },
            {
              "name": "address",
              "type": "String",
              "path": "$.address"
            },
            {
              "name": "age",
              "type": "Integer",
              "path": "$.age"
            }
          ]
        },
        "predicates": [
          {
            "generatorClass": "kaspar.dataload.predicate.OffsetPredicateGenerator",
            "type": "SEGMENT",
            "config": {
              "predicateType": "GreaterThan",
              "partitionThresholds" : [
                { "partition": 0, "threshold": -1 },
                { "partition": 1, "threshold": -1 },
                { "partition": 2, "threshold": -1 },
                { "partition": 3, "threshold": -1 },
                { "partition": 4, "threshold": -1 },
                { "partition": 5, "threshold": -1 }
              ]
            }
          }
        ]
      }
    }

Example Response:

.. sourcecode:: bash

    {"tableId":1}

GET /table?tableName={tableName}
################################

Get table details by name

Example Response:

.. sourcecode:: bash

    {
	  "tableId": 1,
	  "tableName": "Customers_json",
	  "tableSpec": "{
        \"deserializerClass\":\"kaspar.dataload.structure.PathJsonValueRowDeserializer\",
        \"config\":{
          \"columns\":[
            {
		      \"name\":\"id\",
              \"type\":\"Integer\",
              \"path\":\"$.id\"
            },
		    {
		      \"name\":\"name\",
              \"type\":\"String\",
              \"path\":\"$.name\"
            },
		    {
		      \"name\":\"address\",
              \"type\":\"String\",
              \"path\":\"$.address\"
            },
	        {
		      \"name\":\"age\",
              \"type\":\"Integer\",
              \"path\":\"$.age\"
            }
          ]
        },
	    \"predicates\":[
          {
		    \"generatorClass\":\"kaspar.dataload.predicate.OffsetPredicateGenerator\",
            \"type\":\"SEGMENT\",
            \"config\":{
              \"predicateType\":\"GreaterThan\",
              \"partitionThresholds\":[
                {\"partition\":0,\"threshold\":-1},
			    {\"partition\":1,\"threshold\":-1},
			    {\"partition\":2,\"threshold\":-1},
			    {\"partition\":3,\"threshold\":-1},
			    {\"partition\":4,\"threshold\":-1},
			    {\"partition\":5,\"threshold\":-1}
		      ]
		    }
	      }
        ]
      }"
    }

GET /table?tableId={table id}
#############################

Get table details by id

Example Response:

.. sourcecode:: bash

    {
	  "tableId": 1,
	  "tableName": "Customers_json",
	  "tableSpec": "{
        \"deserializerClass\":\"kaspar.dataload.structure.PathJsonValueRowDeserializer\",
        \"config\":{
          \"columns\":[
            {
		      \"name\":\"id\",
              \"type\":\"Integer\",
              \"path\":\"$.id\"
            },
		    {
		      \"name\":\"name\",
              \"type\":\"String\",
              \"path\":\"$.name\"
            },
		    {
		      \"name\":\"address\",
              \"type\":\"String\",
              \"path\":\"$.address\"
            },
	        {
		      \"name\":\"age\",
              \"type\":\"Integer\",
              \"path\":\"$.age\"
            }
          ]
        },
	    \"predicates\":[
          {
		    \"generatorClass\":\"kaspar.dataload.predicate.OffsetPredicateGenerator\",
            \"type\":\"SEGMENT\",
            \"config\":{
              \"predicateType\":\"GreaterThan\",
              \"partitionThresholds\":[
                {\"partition\":0,\"threshold\":-1},
			    {\"partition\":1,\"threshold\":-1},
			    {\"partition\":2,\"threshold\":-1},
			    {\"partition\":3,\"threshold\":-1},
			    {\"partition\":4,\"threshold\":-1},
			    {\"partition\":5,\"threshold\":-1}
		      ]
		    }
	      }
        ]
      }"
    }

GET /table
##########

Get table details for all tables

Example Response:

.. sourcecode:: bash

    {
	  "getTableSpecResponses": [
        {
		  "tableId": 1,
		  "tableName": "Customers_json",
          "tableSpec": "{
            \"deserializerClass\":\"kaspar.dataload.structure.PathJsonValueRowDeserializer\",
            \"config\":{
              \"columns\":[
                {
		          \"name\":\"id\",
                  \"type\":\"Integer\",
                  \"path\":\"$.id\"
                },
		        {
		          \"name\":\"name\",
                  \"type\":\"String\",
                  \"path\":\"$.name\"
                },
		        {
                  \"name\":\"address\",
                  \"type\":\"String\",
                  \"path\":\"$.address\"
                },
	            {
		          \"name\":\"age\",
                  \"type\":\"Integer\",
                  \"path\":\"$.age\"
                }
              ]
            },
	        \"predicates\":[
              {
		        \"generatorClass\":\"kaspar.dataload.predicate.OffsetPredicateGenerator\",
                \"type\":\"SEGMENT\",
                \"config\":{
                   \"predicateType\":\"GreaterThan\",
                   \"partitionThresholds\":[
                     {\"partition\":0,\"threshold\":-1},
			         {\"partition\":1,\"threshold\":-1},
			         {\"partition\":2,\"threshold\":-1},
			         {\"partition\":3,\"threshold\":-1},
			         {\"partition\":4,\"threshold\":-1},
			         {\"partition\":5,\"threshold\":-1}
		           ]
		        }
	          }
            ]
          }"
        }
      ]
    }

------------
Version APIs
------------

GET /version
############

Returns the Kaspar Server version

Example Response:

.. sourcecode:: bash

    { "version": "0.9" }


Quick Start - Creating scala applications with Kaspar
-----------------------------------------------------

The quick start is docker based so no external installation is required.

#. Start Services:

    .. sourcecode:: bash

        docker-compose up -d

#. Input some source data:

    .. sourcecode:: bash

        docker-compose exec master bash
        cd /home/ubuntu/bin
        ./setup.sh

#. Launch Kaspar Server

    .. sourcecode:: bash

        docker-compose exec master bash
        cd /home/ubuntu/bin
        ./launchServer.sh ../kaspar_server/src/main/resources/kaspar.properties

#. Check the version

    .. sourcecode:: bash

        curl -X GET http://localhost:8888/version

    This should return:

    .. sourcecode:: bash

        { "version": "0.9" }

#. Create a table

    .. sourcecode:: bash

        cat > /tmp/Customers_json.spec << EOF
        {
            "tableName" : "Customers_json",
            "tableSpec" : {
               "deserializerClass": "kaspar.dataload.structure.PathJsonValueRowDeserializer",
               "config": {
                    "columns" : [
                        {
                            "name": "id",
                            "type": "Integer",
                            "path": "$.id"
                        },
                        {
                            "name": "name",
                            "type": "String",
                            "path": "$.name"
                        },
                        {
                            "name": "address",
                            "type": "String",
                            "path": "$.address"
                        },
                        {
                            "name": "age",
                            "type": "Integer",
                            "path": "$.age"
                        }
                    ]
                },
                "predicates": [
                    {
                        "generatorClass": "kaspar.dataload.predicate.OffsetPredicateGenerator",
                        "type": "SEGMENT",
                        "config": {
                            "predicateType": "GreaterThan",
                            "partitionThresholds" : [
                                 { "partition": 0, "threshold": -1 },
                                 { "partition": 1, "threshold": -1 },
                                 { "partition": 2, "threshold": -1 },
                                 { "partition": 3, "threshold": -1 },
                                 { "partition": 4, "threshold": -1 },
                                 { "partition": 5, "threshold": -1 }
                            ]
                        }
                    }
                ]
            }
        }
        EOF
        curl -X POST http://localhost:8888/table -d @/tmp/Customers_json.spec

#. Query the created table

    .. sourcecode:: bash

        curl -X POST http://localhost:8888/query -d 'SELECT * FROM Customers_json'

    This should return a query id like:

    .. sourcecode:: bash

        {"queryId":"cb3482ed_b1e5_4fd7_a863_5e94f2c55792"}

#. Get the status of the query

    Using the query id above you can fetch the status of the submitted query:

    .. sourcecode:: bash

        curl -X GET localhost:8888/query?queryId=cb3482ed_b1e5_4fd7_a863_5e94f2c55792

    This should return:

    .. sourcecode:: bash

        {"queryId":"cb3482ed_b1e5_4fd7_a863_5e94f2c55792","status":"COMPLETE"}

#. Get the query results

    .. sourcecode:: bash

        curl -X GET localhost:8888/result?queryId=cb3482ed_b1e5_4fd7_a863_5e94f2c5579

    This should return:

    .. sourcecode:: bash

        {
	      "getRowResponses": [{
		    "getColumnResponses": [{
    			"name": "_offset",
    			"value": "0"
    		  }, {
    			"name": "_partition",
    			"value": "0"
    		  }, {
    			"name": "_timestamp",
    			"value": "1635022725415"
    		  }, {
    			"name": "id",
    			"value": "1"
    		  }, {
    			"name": "name",
    			"value": "Harry Potter"
    		  }, {
    			"name": "address",
    			"value": "4 Privet Drive"
    		  }, {
    			"name": "age",
    			"value": "11"
    		  }]
    	  }, {
    		"getColumnResponses": [{
    			"name": "_offset",
    			"value": "1"
    		  }, {
	    		"name": "_partition",
		    	"value": "0"
		      }, {
    			"name": "_timestamp",
	    		"value": "1635022725456"
		      }, {
    			"name": "id",
	    		"value": "4"
		      }, {
    			"name": "name",
	    		"value": "Buffy Summers"
		      }, {
    			"name": "address",
	    		"value": "1630 Revello Drive"
		      }, {
    			"name": "age",
	    		"value": "27"
		      }]
	      }, {
		    "getColumnResponses": [{
    			"name": "_offset",
	    		"value": "2"
	  	      }, {
    			"name": "_partition",
	    		"value": "0"
		      }, {
    			"name": "_timestamp",
	    		"value": "1635022725460"
              }, {
    			"name": "id",
	    		"value": "7"
		      }, {
    			"name": "name",
	    		"value": "Fox Mulder"
		      }, {
    			"name": "address",
	    		"value": "2630 Hegal Place"
		      }, {
    			"name": "age",
	    		"value": "35"
		      }]
	      }, {
    		"getColumnResponses": [{
	    		"name": "_offset",
		    	"value": "0"
		      }, {
    			"name": "_partition",
	    		"value": "2"
    		  }, {
	    		"name": "_timestamp",
		    	"value": "1635022725416"
    		  }, {
	    		"name": "id",
		    	"value": "2"
    		  }, {
	    		"name": "name",
		    	"value": "Arthur Dent"
		      }, {
    			"name": "address",
	    		"value": "155 Country Lane"
		      }, {
    			"name": "age",
	    		"value": "42"
    		  }]
	      }, {
    		"getColumnResponses": [{
	    		"name": "_offset",
		    	"value": "1"
		      }, {
    			"name": "_partition",
	    		"value": "2"
		      }, {
    			"name": "_timestamp",
	    		"value": "1635022725457"
		      }, {
    			"name": "id",
	    		"value": "5"
		      }, {
    			"name": "name",
	    		"value": "Norm"
		      }, {
    			"name": "address",
	    		"value": "84 Beacon Street"
		      }, {
    			"name": "age",
	    		"value": "40"
		      }]
	      }, {
		    "getColumnResponses": [{
    			"name": "_offset",
	    		"value": "2"
		      }, {
			    "name": "_partition",
    			"value": "2"
	    	  }, {
		    	"name": "_timestamp",
			    "value": "1635022725461"
    		  }, {
	    		"name": "id",
		    	"value": "8"
    		  }, {
	    		"name": "name",
		    	"value": "Charles Xavier"
		      }, {
    			"name": "address",
	    		"value": "1407 Graymalkin Lane"
		      }, {
    			"name": "age",
	    		"value": "80"
		      }]
    	  }, {
	    	"getColumnResponses": [{
		    	"name": "_offset",
    			"value": "0"
	    	  }, {
		    	"name": "_partition",
			    "value": "4"
    		  }, {
	    		"name": "_timestamp",
		    	"value": "1635022725375"
		      }, {
    			"name": "id",
	    		"value": "0"
		      }, {
    			"name": "name",
	    		"value": "Bruce Wayne"
		      }, {
    			"name": "address",
	    		"value": "Wayne Manor"
		      }, {
    			"name": "age",
	    		"value": "36"
		      }]
	      }, {
		    "getColumnResponses": [{
    			"name": "_offset",
	    		"value": "1"
		      }, {
    			"name": "_partition",
	    		"value": "4"
		      }, {
			    "name": "_timestamp",
    			"value": "1635022725416"
	    	  }, {
		    	"name": "id",
			    "value": "3"
    		  }, {
	    		"name": "name",
		    	"value": "Clark Kent"
		      }, {
    			"name": "address",
	    		"value": "344 Clinton Street, Apartment 3D"
		      }, {
    			"name": "age",
	    		"value": "24"
		      }]
	      }, {
    		"getColumnResponses": [{
	    		"name": "_offset",
		    	"value": "2"
		      }, {
    			"name": "_partition",
	    		"value": "4"
		      }, {
    			"name": "_timestamp",
	    		"value": "1635022725459"
		      }, {
			    "name": "id",
    			"value": "6"
	    	  }, {
		    	"name": "name",
			    "value": "Spongebob Squarepants"
    		  }, {
	    		"name": "address",
		    	"value": "124 Conch Street"
		      }, {
    			"name": "age",
	    		"value": "34"
		      }]
	      }, {
		    "getColumnResponses": [{
    			"name": "_offset",
	    		"value": "3"
		      }, {
			    "name": "_partition",
    			"value": "4"
	    	  }, {
		    	"name": "_timestamp",
			    "value": "1635022725462"
		      }, {
    			"name": "id",
	    		"value": "9"
		      }, {
			    "name": "name",
			    "value": "Peter Parker"
    		  }, {
	    		"name": "address",
		    	"value": "20 Ingram Street"
		      }, {
    			"name": "age",
	    		"value": "17"
		      }]
	      }]
        }

