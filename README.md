# CC5212-Proyecto

## CC5212 - Procesamiento Masivo de Datos (Big Data) | Proyecto Semestral

### Mario González O., Bastián Pezoa V. & Michael Scott S. -- Grupo 16

<br>

Para este proyecto se utilizo el dataset *["The Reddit Climate Change Dataset"](https://www.kaggle.com/datasets/pavellexyr/the-reddit-climate-change-dataset?select=the-reddit-climate-change-dataset-comments.csv)*. Este contiene distintos comentarios y publicaciones realizadas por usuarios de Reddit desde el año 2017 hasta la fecha del 1 de Septiembre del 2022, en el cual se mencionen los terminos "climate" y "change". El dataset contiene 2 archivos .csv, uno con los comentarios y otro con los posts. Para este proyecto se utilizo el archivo de comentarios el cual contiene, la fecha de creación, el nombre del subreddit donde se realizo el comentario y si este es "Not Safe For Work (NSFW)", una puntuación que define el "sentimiento" del comentario y la cantidad de upvotes (o score) del comentario.

Este proyecto busca capturar información acerca de las correlaciones y tendencias entre el sentimiento de los diferentes comentarios en los subreddits y las diferentes variables que el dataset contiene. Para ello se llevan a cabo métodos de procesamiento masivo de datos mediante el framework de Apache Spark y su API para el lenguaje Python (PySpark).

Para esclarecer, los comentarios con un puntaje de sentimiento entre [-0.25, 0.25] son considerados neutros. Entre [-1, -0.25] son considerados negativos, o negacionistas, y entre [0.25, 1] son considerados positivos.

## Consultas

Las consultas realizadas fueron las siguientes:

- ¿Ha existido un aumento en los comentarios negacionistas a lo largo del tiempo?
- ¿Existe alguna relación entre el puntaje de sentimiento y la cantidad de upvotes?
- ¿Cuál ha sido el promedio de puntaje de sentimiento a lo largo de los años?
- ¿Cuáles son las estadisticas sobre el puntaje de sentimiento en cada subreddit?
- ¿Cuál es la cantidad de comentarios neutros, negativos y positivos en cada subreddit?
- ¿Hay alguna diferencia entre el promedio de puntaje de sentimiento de los comentarios NSFW y los que no lo son, a lo largo de los años?

Cada una de estas consultas se busca responder en los archivos .py de este repositorio mediante la utilización de DataFrames de PySpark.
