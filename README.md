# Kafka Connect     Connector for Azure Data Lake
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud?ref=badge_shield)


**kafka-connect-adl** is kafka connect sink connector 
designed to be used to copy data between Kafka and azure data lake.

# Usage

*kafka-connect-adl* is used normally like other connectors.

For using it Clone this repo on your system.Ensure maven is installed on your system for building it.

Go to root directory of the project and run 

    mvn clean install

In the target folder you will see **kafka-connect-adl-4.1.1-package** directory which contains the connector jars in the way kafka maintains it.

Copy the contents of this directory to KAFKA_HOME or KAFKA_BROKER_HOME .

**Note** If you are using confluent copy the contents in CONFLUENT_HOME directory.

## Verify

To verify you have correctly installed the connector run

    curl http//:<<KAFKA_BROKER_HOST>>:8083/connector/connector-plugins

**Note** Replace *KAFKA_BROKER_HOST* with the IP_ADDRESS of the host.
 
You will see jk.bigdata.tech.AdlSinkConnector listed in the output.

## Running the connector

For running the connector you need to prepare a configuration file for kafka-connect.
You can look out for sample configuration files in **config** folder.

## Configuration Paramters :

- adl.data.lake.name=<< AZURE DATA LAKE NAME >>.azuredatalakestore.net
- adl.client.id=<< AZURE ACTIVE DIRECTORY CLIENT ID >>
- adl.client.secret=<< AZURE ACTIVE DIRECTORY APP KEY >>
- adl.token.endpoint=<< AZURE ACTIVE DIRECTORY TOKEN ENDPOINT >>

These are configuration parameters most importantly required by kafka-connect-adl connector.
 
For creating Azure data lake refer [here](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-get-started-portal)

For generating client ID and secret and getting token endpoint refer [here]()

After filling in the values in the configuration file.Run this

    curl -X POST -H "Accept: application/json" -H "Content-Type: application/json" --data @<<FILE_NAME>>.json http://<<KAFKA_BROKER_HOST>>:8083/connectors
    
Congratulation your connector is running.Check its status using

    curl http://<<KAFKA_BROKER_HOST>>:8083/connectors/<<CONNECTOR_NAME_IN_CONFIGURATION_FILE>>/status


# Issues

- Issue Tracker: https://github.com/jksinghpro/kafka-connect-adl/issues


# License

The project is licensed under the Apache 2 license.

