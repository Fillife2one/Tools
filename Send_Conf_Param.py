import os
from dotenv import load_dotenv
import message_pb2
from kafka import KafkaProducer
import ssl

load_dotenv()
KAFKA_USERNAME = os.environ["KAFKA_USERNAME"]
KAFKA_PASSWORD = os.environ["KAFKA_PASSWORD"]

# Заполняем сообщение
message = message_pb2.SetNewCustomerConfigurationResponse()
message.vehicle_id = "DW8ZZ9NT7B89R7PVG"   # Вин авто
message.set_customer_configuration_result = 5  # Статус конфигурации авто(Значение от 1 - 11)
message.aux_data = "ok"  # Комментарий

binary_message = message.SerializeToString()  # Кодирование в бинарный формат

# Использование SSL-сертификата
ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(r'C:\Users\user\PycharmProjects\Cert\CA.pem')

producer = KafkaProducer(
    bootstrap_servers=['rc1a-790iirs3ofg0goju.mdb.yandexcloud.net:9091'],
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='team-internal-integrations',
    sasl_plain_password='VndFl2amdLpu65b',
    ssl_context=ssl_context  
)

producer.send('external-integrations.config-report-response.dev', value=binary_message) #Название топика
producer.flush()
producer.close()
print("Сообщение отправлено!")
