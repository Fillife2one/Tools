import os
from dotenv import load_dotenv
import message_pb2
from kafka import KafkaProducer
import ssl

#load_dotenv()
#KAFKA_USERNAME = os.environ['KAFKA_USERNAME']
#KAFKA_PASSWORD = os.environ['KAFKA_PASSWORD']
#BROKER_DEV_TEST=os.environ['BROCKER_DEV_TEST']

# Заполняем сообщение
message = message_pb2.SetNewCustomerConfigurationResponse()
message.vehicle_id = "1YF93SBJHLYYTCZVL"   # Ввести vin авто
message.set_customer_configuration_result = 7 # Статус конфигурации авто, значение от 1 - 11
message.aux_data = "ok"  # Комментарий (опционально)

binary_message = message.SerializeToString()  # Кодирование в бинарный формат

# Использование SSL-сертификата
ssl_context = ssl.create_default_context()
# Скачать сертификат из инструкции и прописать к нему актуальный путь
ssl_context.load_verify_locations(r'C:\Users\user\PycharmProjects\ConfState\resourse\CA.pem')

producer = KafkaProducer(
    bootstrap_servers=['BROKER'],  # Получить в Vault
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='login', # Получить в Vault
    sasl_plain_password='password', # Получить в Vault
    ssl_context=ssl_context  
)

producer.send('external-integrations.config-report-response.test', value=binary_message) #Название топика
producer.flush()
producer.close()
print("Сообщение отправлено!")
