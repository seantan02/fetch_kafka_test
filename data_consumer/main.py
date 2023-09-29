import helper
import json

if __name__ == "__main__":
    try:
        #Create consumer
        kafka_consumer = helper.create_kafka_consumer("user-login")
        kafka_consumer_status = kafka_consumer[0]
        kafka_consumer_object = kafka_consumer[1]
        assert kafka_consumer_status == True, f"Kafka consumer failed to be created due to {kafka_consumer_object}"
        #Create a producer 
        kafka_producer = helper.create_kafka_producer()
        kafka_producer_status = kafka_producer[0]
        kafka_producer_object = kafka_producer[1]
        assert kafka_producer_status == True, f"Kafka producer failed to be created due to {kafka_producer_object}"
        #Retrieve from consumer then send with producer
        for msg in kafka_consumer_object:
            message_value = msg.value
            #I had to use .get() because the data is inconsistent that, for example some may have missing device type
            user_id = message_value.get("user_id", "")
            app_version = message_value.get("app_version", "")
            ip = message_value.get("ip", "")
            locale = message_value.get("locale", "")
            device_id = message_value.get("device_id", "")
            timestamp = message_value.get("timestamp", "")
            device_type = message_value.get("device_type", "")
            #Assume user_id, and device_type are sensitive and we want to keep it safe
            #Let's encrypt it with key
            #Let's also convert timestamp from unix to SQL datetime format
            key = helper.generate_key()
            user_id = helper.encrypt_data(key, user_id)
            ip = helper.encrypt_data(key, ip)
            device_id = helper.encrypt_data(key, device_id)
            #Now let's process timestamp
            datetime = helper.convert_unix_to_datetime(timestamp=timestamp)
            #Combine the data 
            processed_data = {
                "user_id" : str(user_id),
                "app_version" : str(app_version),
                "ip" : str(ip),
                "locale" : str(locale),
                "device_id" : str(device_id),
                "timestamp" : str(timestamp),
                "datetime": str(datetime),
                "device_type" : str(device_type)
            }
            processed_data = json.dumps(processed_data)
            assert helper.kafka_producer_send(kafka_producer_object, "user-login-processed", processed_data) == True
    except Exception as e:
        print(f"Error occured: {e}")
    finally:
        if "kafka_consumer_object" in locals():
            kafka_consumer_object.close()
        if "kafka_producer_object" in locals():
            kafka_producer_object.close()