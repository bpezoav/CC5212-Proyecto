# CC5212 - Procesamiento Masivo de Datos/Big Data | Proyecto Semestral
## Mario González O., Bastián Pezoa V. & Michael Scott S. - Grupo 16

Este proyecto utiliza el dataset de The Reddit Climate Change Dataset (https://www.kaggle.com/datasets/pavellexyr/the-reddit-climate-change-dataset). Se llevan a cabo métodos de procesamiento masivo de datos mediante el framework de Apache Spark y su API en Python (PySpark).

Se hacen queries para capturar información acerca de las correlaciones y tendencias entre el sentimiento de los comentarios en los distintos subreddits y las otras variables disponibles en el dataset (por ejemplo, se visualizan cuáles son los subreddits con comentarios más negativos, entre otras queries).

Cabe destacar que el dataset ya viene con una variable entre -1 y 1 que clasifica cada comentario en un sentimiento en específico, sobre la cual se consideró:
- Negativo si el sentimiento está entre -1 y -0.25
- Negativo si el sentimiento está entre -0.25 y 0.25
- Negativo si el sentimiento está entre 0.25 y 1
