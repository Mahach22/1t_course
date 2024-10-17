from pyspark.sql import SparkSession
import random
from faker import Faker

spark = SparkSession.builder \
    .appName("register") \
    .getOrCreate()
# Список возможных товаров для генерации в поле Продукт
product_names = [
    "Смартфон", "Ноутбук", "Беспроводные наушники", "Умные часы", "Планшет", "Портативная колонка", "Фитнес-браслет", "Игровая консоль", "Внешний жесткий диск", "Цифровая камера",
    "Робот-пылесос", "Электрическая зубная щетка", "Соковыжималка", "Парогенератор", "Стиральная машина", "Холодильник", "Микроволновая печь", "Сенсорная лампа", "Электрочайник",
]
# случайная генерация даты в пределах текущего дня и 20 последних лет
fake = Faker()
def data_pokupki():
    random_date = fake.date_between(start_date='-20y', end_date='today')
    return random_date
# добавление возможности изменять количество сгенерированных строк. Минимальное количество - 1000 строк.
count = int(0)
while count < 1000:
    count = int(input("Введите количество строк, которые хотите сгенерировать(но не менее 1000): "))
# Создайте данные : Дата, UserID, Продукт, Количество, Цена.
pokupki_data = [(i, data_pokupki(), random.randint(1, 301), random.choice(product_names), random.randint(1, 20), random.randint(600, 50000)) for i in range(1, count+1)]
pokupki_df = spark.createDataFrame(pokupki_data, ["id", "Дата", "UserID", "Продукт", "Количество", "Цена"])

pokupki_df.coalesce(1).write.option("header", "true").csv("pokupki.csv")

pokupki_df.show()
spark.stop()