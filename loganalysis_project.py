#! /usr/bin/env python

import psycopg2

db_name = "news"


def run_query(query):
    """Connects to the database, runs the query passed to it,
    and returns the results"""
    db = psycopg2.connect('dbname=' + db_name)
    c = db.cursor()
    c.execute(query)
    rows = c.fetchall()
    db.close()
    return rows


def top_articles():
    """Top 3 most read articles"""

    # Build Query String
    query = """
        SELECT articles.title, COUNT(*) AS total
        FROM articles
        JOIN log
        ON log.path LIKE concat('/article/%', articles.slug)
        GROUP BY articles.title
        ORDER BY total DESC
        LIMIT 3;
    """

    # Run Query
    results = run_query(query)

    # Print Results
    print("=================================")
    print(' TOP THREE ARTICLES :')
    print("=================================")
    count = 1
    for i in results:
        print('(' + str(count) + ') "' + i[0] + '" :: ' + str(i[1]) + " views")
        count += 1


def top_article_authors():
    """Top 3 most popular authors"""

    # Build Query String
    query = """
        SELECT authors.name, COUNT(*) AS total
        FROM authors
        JOIN articles
        ON authors.id = articles.author
        JOIN log
        ON log.path like concat('/article/%', articles.slug)
        GROUP BY authors.name
        ORDER BY total DESC
        LIMIT 3;
    """

    # Run Query
    results = run_query(query)

    # Print Results
    print("\n===============================")
    print('TOP THREE AUTHORS:')
    print("=================================")

    count = 1
    for i in results:
        print('(' + str(count) + ') ' + i[0] + ' :: ' + str(i[1]) + " views")
        count += 1


def days_with_errors():
    """returns days with more than 1% errors"""

    # Build Query String
    query = """
        SELECT total.day,
          ROUND(((errors.error_requests*1.0) / total.requests), 3) AS Percent
        FROM (
          SELECT date_trunc('day', time) "day", count(*) AS Error_Requests
          FROM log
          WHERE status LIKE '404%'
          GROUP BY day
        ) AS errors
        JOIN (
          SELECT date_trunc('day', time) "day", count(*) AS Requests
          FROM log
          GROUP BY day
          ) AS total
        ON total.day = errors.day
        WHERE (ROUND(((errors.Error_Requests*1.0) / total.Requests), 3) > 0.01)
        ORDER BY Percent DESC;
    """

    # Run Query
    results = run_query(query)

    # Print Results
    print("\n=================================")
    print('DAYS WITH MORE THAN 1% ERRORS:')
    print("=================================")

    for i in results:
        print(i[0].strftime('%B %d, %Y') + " -- " + str(round(i[1]*100, 1)) + "%" + " errors")

print('\nPlease wait we are procession your results...\n')
top_articles()
top_article_authors()
days_with_errors()
