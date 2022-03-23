from recipe_scrapers import scrape_me

import mysql.connector
import re


def mysqlConnect(host, port, user, passw, database):
  try:
    con = mysql.connector.connect(
      host = host,
      port = port,
      user = user,
      password = passw,
      database = database
    )
    return con
  except:
    return "Error"

def createTable(con):
  cur = con.cursor()
  cur.execute("""CREATE TABLE if not exists """ + table + """ (
      url VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
      title VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
      totalTime VARCHAR(255),
      yields VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
      ingredients LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
      instructions LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
      image VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
      calories VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
      fatContent VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
      carbohydrateContent VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
      proteinContent VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    )
  """)

def getData(url):
  try:
    scraper = scrape_me(url, wild_mode=True)
    return scraper
  except:
    return "Error"

def existsDataMysql(con, url):
  cur = con.cursor()
  sql = "SELECT * FROM {} WHERE url = %s".format(table)
  cur.execute(sql, (url,))

  rows = cur.fetchall()

  if len(rows) != 0:
    return True
  else:
    return False

def saveToCSV(con):
  cur = con.cursor()
  cur.execute("select * from " + table)

  header = (row[0] for row in cur.description)

  rows = cur.fetchall()

  f = open("file.csv", "w", encoding = "utf-8", newline = "")

  f.write("|".join(header) + "\n")

  for row in rows:
    f.write("|".join(str(r) for r in row) + "\n")

  f.close()

def updateDataMysql(con, scraper, url):
  foundNutrient = False

  title = scraper.title()
  totalTime = scraper.total_time()
  yields = scraper.yields()
  ingredients = scraper.ingredients()
  instructions = scraper.instructions()
  image = scraper.image()
  nutrients = scraper.nutrients()

  if title is None:
    title = ""
  if totalTime is None:
    totalTime = ""
  if yields is None:
    yields = ""
  if ingredients is None:
    ingredients = ""
  if instructions is None:
    instructions = ""
  if image is None:
    image = ""
  if nutrients is None:
    nutrients = ""

  if len(nutrients) > 0:
    try:
      calories = nutrients["calories"].replace("calories", "kcal")
      foundNutrient = True
    except:
      calories = ""
    try:
      fatContent = nutrients["fatContent"].replace("grams fat", "g")
      foundNutrient = True
    except:
      fatContent = ""
    try:
      carbohydrateContent = nutrients["carbohydrateContent"].replace("grams carbohydrates", "g")
      foundNutrient = True
    except:
      carbohydrateContent = ""
    try:
      proteinContent = nutrients["proteinContent"].replace("grams protein", "g")
      foundNutrient = True
    except:
      proteinContent = ""
  else:
    calories = ""
    fatContent = ""
    carbohydrateContent = ""
    proteinContent = ""

  if foundNutrient:
    if isinstance(ingredients, list):
      ingredientsTemp = ""
      for i in range(len(ingredients)):
        if i != len(ingredients) - 1:
          ingredientsTemp += ingredients[i] + ", "
        else:
          ingredientsTemp += ingredients[i]
      ingredients = ingredientsTemp

    instructions = re.sub("\n|\r|\t", " ", instructions)

    valDict = {
      "url": url,
      "title": title,
      "totalTime": totalTime,
      "yields": yields,
      "ingredients": ingredients,
      "instructions": instructions,
      "image": image,
      "calories": calories,
      "fatContent": fatContent,
      "carbohydrateContent": carbohydrateContent,
      "proteinContent": proteinContent
    }

    sql = "UPDATE {} SET {} WHERE url = %s"
    sql = sql.format(table, ", ".join("{}=%s".format(k) for k in valDict))

    cur = con.cursor()
    valDictT = list(valDict.values())
    valDictT.append(url)

    cur.execute(sql, valDictT)
    return 'Ok'
  else:
    return 'Error'

def insertDataMysql(con, scraper, url):
  foundNutrient = False

  title = scraper.title()
  totalTime = scraper.total_time()
  yields = scraper.yields()
  ingredients = scraper.ingredients()
  instructions = scraper.instructions()
  image = scraper.image()
  nutrients = scraper.nutrients()

  if title is None:
    title = ""
  if totalTime is None:
    totalTime = ""
  if yields is None:
    yields = ""
  if ingredients is None:
    ingredients = ""
  if instructions is None:
    instructions = ""
  if image is None:
    image = ""
  if nutrients is None:
    nutrients = ""

  if len(nutrients) > 0:
    try:
      calories = nutrients["calories"].replace("calories", "kcal")
      foundNutrient = True
    except:
      calories = ""
    try:
      fatContent = nutrients["fatContent"].replace("grams fat", "g")
      foundNutrient = True
    except:
      fatContent = ""
    try:
      carbohydrateContent = nutrients["carbohydrateContent"].replace("grams carbohydrates", "g")
      foundNutrient = True
    except:
      carbohydrateContent = ""
    try:
      proteinContent = nutrients["proteinContent"].replace("grams protein", "g")
      foundNutrient = True
    except:
      proteinContent = ""
  else:
    calories = ""
    fatContent = ""
    carbohydrateContent = ""
    proteinContent = ""

  if foundNutrient:
    if isinstance(ingredients, list):
      ingredientsTemp = ""
      for i in range(len(ingredients)):
        if i != len(ingredients) - 1:
          ingredientsTemp += ingredients[i] + ", "
        else:
          ingredientsTemp += ingredients[i]
      ingredients = ingredientsTemp

    instructions = re.sub("\n|\r|\t", " ", instructions)

    valDict = {
      "url": url,
      "title": title,
      "totalTime": totalTime,
      "yields": yields,
      "ingredients": ingredients,
      "instructions": instructions,
      "image": image,
      "calories": calories,
      "fatContent": fatContent,
      "carbohydrateContent": carbohydrateContent,
      "proteinContent": proteinContent
    }

    placeholders = ', '.join(['%s'] * len(valDict))
    columns = ', '.join(valDict.keys())
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, columns, placeholders)

    cur = con.cursor()
    cur.execute(sql, list(valDict.values()))
    con.commit()
    return 'Ok'
  else:
    return 'Error'

if __name__ == '__main__':
  host = "host"
  port = "port"
  user = "user"
  password = "password"
  database = "database"
  table = "table"

  con = mysqlConnect(host, port, user, password, database)

  if con != "Error":
    createTable(con)

    f = open("urls.txt", "r")
    urls = f.read().splitlines()
    f.close()

    for url in urls:
      if url.strip() != "":
        url = url.strip()
        scraper = getData(url)
        if scraper != "Error":
          if not existsDataMysql(con, url):
            m = insertDataMysql(con, scraper, url)
            if m == "Error":
              print("No nutritional value ->", url)
              continue
            else:
              print("DONE ->", url)
          else:
            m = updateDataMysql(con, scraper, url)
            if m == "Error":
              print("No nutritional value ->", url)
              continue
            else:
              print("DONE ->", url)
          saveToCSV(con)
        else:
          print("Error URL:", url)

  else:
    print("Error connecting to database!")

