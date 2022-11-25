# подключаем SQLite
import pprint
import sqlite3 as sl

source_dict = [{'name': 'Удавы и питоны Уход и содержание',
                'author': ['Крицкий А.'],
                'year': 2009,
                'publisher': 'Профиздат'},
               {'name': 'Алгоритмы неформально. Инструкция для ' +
                        'начинающих питонистов',
                'author': ['Брэдфорд Такфилд'],
                'year': '2022',
                'publisher': 'Питер'},
               {'name': 'Изучаем Python. Том 1',
                'author': ['Марк Лутц'],
                'year': '2019',
                'publisher': 'Диалектика'},
               {'name': 'Изучаем Python. Том 2',
                'author': ['Марк Лутц'],
                'year': '2020',
                'publisher': 'Вильямс'},
               {'name': 'Искусственный интеллект и компьютерное зрение. ' +
                        'Реальные проекты на Python, Keras и TensorFlow',
                'author': ['Коул Анирад',
                           'Казам Мехер',
                           'Ганджу Сиддха'],
                'year': '2023',
                'publisher': 'Питер'},
               {'name': 'Высокопроизводительные Python-приложения. ' +
                        'Практическое руководство по эффективному ' +
                        'программированию',
                'author': ['Горелик Миша',
                           'Йен Освальд'],
                'year': '2022',
                'publisher': 'Бомбора'},
               {'name': 'Учим Python, делая крутые игры',
                'author': ['Эл Свейгарт'],
                'year': '2021',
                'publisher': 'Бомбора'},
               {'name': 'Автоматизация рутинных задач с помощью Python',
                'author': ['Эл Свейгарт'],
                'year': '2021',
                'publisher': 'Диалектика'},
               {'name': 'Простой Python. Современный стиль ' +
                        'программирования',
                'author': ['Билл Любанович'],
                'year': '2021',
                'publisher': 'Питер'},
               {'name': 'Простой Python. Современный стиль ' +
                        'программирования',
                'author': ['Билл Любанович'],
                'year': '2016',
                'publisher': 'Питер'},
               {'name': 'Цель на 360. Управляй судьбой',
                'author': ['Пелехатый М.М.',
                           'Спирица Е.'],
                'year': '2023',
                'publisher': 'Питер'}]

if __name__ == '__main__':
    # Часть 12.14: Объединение таблиц в SQL и базах данных SQLite: JOIN и SELECT
    # https://zametkinapolyah.ru/zametki-o-mysql/chast-12-14-obedinenie-tablic-v-sql-i-bazax-dannyx-sqlite-join-i-select.html
    # Как подружить Python и базы данных SQL. Подробное руководство
    # https://proglib.io/p/kak-podruzhit-python-i-bazy-dannyh-sql-podrobnoe-rukovodstvo-2020-02-27
    # Создаём и наполняем базу данных SQLite в Python
    # https://thecode.media/sqlite-py/

    # Открываем файл с базой данных.
    # При выполнении команды connect файл books.sqlite либо
    # будет открыт, либо будет создан, если он не существует
    # con = sl.connect('books.sqlite')

    # Методы execute, executemany, executescript, commit, rollback
    # Источник: https://proproprogs.ru/modules/metody-execute-executemany-executescript-commit-rollback
    # Когда контекстный менеджер завершает свою работу,
    # он автоматически выполняет два метода:
    #     con.commit() – применение всех изменений в таблицах БД;
    #     con.close() – закрытие соединения с БД.
    # Это необходимые действия для сохранения внесенных изменений в БД.
    sqlite_db_name = "books.sqlite"
    with sl.connect(sqlite_db_name) as con:
        cur = con.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS 
                        Authors (
                            Id          INTEGER PRIMARY KEY,
                            Author_Name TEXT
                            )
                    """)
        cur.execute("""CREATE TABLE IF NOT EXISTS 
                        Books (
                            Id               INTEGER PRIMARY KEY,
                            Book_Name        TEXT,
                            Publication_Year INTEGER,
                            Publisher_Id     INTEGER REFERENCES Publishers (Id)
                            )
                    """)
        cur.execute("""CREATE TABLE IF NOT EXISTS 
                        Publishers (
                            Id             INTEGER PRIMARY KEY,
                            Publisher_Name TEXT
                            )
                    """)
        cur.execute("""CREATE TABLE IF NOT EXISTS 
                       Books_Authors (
                            Id        INTEGER PRIMARY KEY,
                            Book_Id   INTEGER REFERENCES Books (Id),
                            Author_Id INTEGER REFERENCES Authors (Id) 
                            )
                    """)

    con = None
    try:
        con = sl.connect(sqlite_db_name)
        cur = con.cursor()

        # заполняем таблицы Publishers и Authors
        cur.execute('BEGIN')  # определяем точку для отмены изменения БД в случае сбоя
        for item in source_dict:
            # заполняем таблицу Publishers
            cur.execute('SELECT * FROM Publishers WHERE Publisher_Name = :publisher', item)
            if not cur.fetchall():  # Если список пуст, значит нет издателя с таким наименованием
                cur.execute('INSERT INTO Publishers (Publisher_Name) VALUES(:publisher)', item)

            # заполняем таблицу Authors
            for author in item['author']:
                # cur.execute('SELECT * FROM Authors WHERE Author_Name = :author', {'author': author})
                # Вариант:
                cur.execute('SELECT * FROM Authors WHERE Author_Name = ?', (author,))

                if not cur.fetchall():  # Если список пуст, значит нет издателя с таким наименованием
                    #  Метод execute принимает в качестве параметров
                    #  кортеж. Запятая нужна после author.
                    cur.execute('INSERT INTO Authors (Author_Name) VALUES(?)', (author,))
        con.commit()  # Зафиксировали изменения в БД (применение всех изменений в таблицах БД)

        # заполняем таблицу Books
        cur.execute('BEGIN')  # определяем точку для отмены изменения БД в случае сбоя
        for item in source_dict:
            cur.execute('SELECT Id FROM Publishers WHERE Publisher_Name = :publisher', item)
            # if len(rows_tpl) == 0:
            # Возвращается список кортежей или пустой список
            # Пример не пустого списка: [(2,)]
            rows_lst = cur.fetchall()
            if len(rows_lst) > 1:
                print("Ошибка заполнения таблицы Books.")
                pprint.pprint(item)
                continue
            elif len(rows_lst) == 0:
                # Если список пуст, значит нет издателя с таким наименованием
                # Теоретически, такая ситуация должна отсутствовать при штатной работе и базой
                cur.execute('INSERT INTO Publishers (Publisher_Name) VALUES(:publisher)', item)
                row_id = cur.lastrowid
            else:
                # Присутствует единственная запись
                # Результат такой: [(2,)]
                row_id = rows_lst[0][0]
            # Проверяем, что такая книга уже имеется.
            # Если книга отсутствует - добавляем. Иначе - пропускаем.
            test_row_id = cur.execute('''SELECT Id FROM Books WHERE (Book_Name = :name) and
                                                               (Publication_Year = :year) and
                                                               (Publisher_Id = :publisher_id)
                                      ''',
                                      {**item, 'publisher_id': row_id}).fetchone()
            # Метод cursor.fetchone() извлекает следующую строку из набора результатов запроса,
            # возвращая одну последовательность или None, если больше нет доступных данных.
            if test_row_id is None:
                cur.execute('''INSERT INTO Books (Book_Name, 
                                                  Publication_Year, 
                                                  Publisher_Id)
                                           VALUES(:name, 
                                                  :year, 
                                                  :publisher_id)
                            ''',
                            # {'name': item['name'],
                            #  'year': item['year'],
                            #  'publisher_id': row_id})
                            #       a = {1: 'aaa', 2: 'bbbb', 3: 'ccccc'}
                            #       print({**a, 'publisher_id': 3})
                            #       Вывод: {1: 'aaa', 2: 'bbbb', 3: 'ccccc', 'publisher_id': 3}
                            {**item, 'publisher_id': row_id})
        con.commit()  # Зафиксировали изменения в БД (применение всех изменений в таблицах БД)

        cur.execute('BEGIN')  # определяем точку для отмены изменения БД в случае сбоя
        # заполняем таблицу Books_Authors
        # Книги требуется искать по совокупности параметров:
        # Book_Name, Publication_Year, Publisher_Id
        # Специфика в том, что книга с одним и тем же наименованием
        # и даже тем же годом может выходить в разных издательствах.
        for item in source_dict:
            # Определили код издателя.
            cur.execute('SELECT Id FROM Publishers WHERE Publisher_Name = :publisher', item)
            # if len(rows_tpl) == 0:
            # rows_lst = cur.fetchall()
            # Считаем, что с базой всё нормально и проверки на внешнее вмешательство не делаем.
            # То есть, запись присутствует и только в единственном числе.
            # Присутствует единственная запись
            # Результат такой: [(2,)]
            row_id = cur.fetchall()[0][0]
            # row_id - это идентификатор (код) издателя в таблице Publishers.
            cur.execute('''SELECT Id FROM Books WHERE (Book_Name = :name) and
                                                      (Publication_Year = :year) and
                                                      (Publisher_Id = :publisher_id)''',
                        {**item, 'publisher_id': row_id})
            # Считаем, что с базой всё нормально и проверки на внешнее вмешательство не делаем.
            # То есть, запись присутствует и только в единственном числе.
            # Результат такой: (1,)
            # В отличии от метода cur.fetchall(), где результат в виде списка кортежей: [(2,)]
            book_id = cur.fetchone()[0]

            # заполняем таблицу Authors
            for author in item['author']:
                cur.execute('SELECT Id FROM Authors WHERE Author_Name = :author', {'author': author})
                author_id = cur.fetchone()[0]
                # Проверяем - имеется ли такая запись или нет.
                # Если отсутствует, добавляем
                cur.execute('''SELECT Id FROM Books_Authors 
                                        WHERE Book_Id = :book_id
                                              AND
                                              Author_Id = :author_id''',
                            {'book_id': book_id, 'author_id': author_id})
                row_id = cur.fetchone()
                if row_id is None:
                    cur.execute('''INSERT INTO Books_Authors (Book_Id, Author_Id)
                                          VALUES(:book_id, :author_id)''',
                                {'book_id': book_id, 'author_id': author_id})
        con.commit()  # Зафиксировали изменения в БД (применение всех изменений в таблицах БД)

    except sl.Error as e:
        if con:
            con.rollback()  # Отменили изменения в БД, так как произошёл сбой.
        print("Ошибка выполнения запроса. Сообщение об ошибке:", e, sep='\n')
    finally:
        if con:
            con.close()

    # обход таблицы и вывод в терминал
    with sl.connect(sqlite_db_name) as con:
        cur = con.cursor()

    # Делаем выборку только книг.
    # Формат выборки: список из кортежей, примерно так:
    # [('Удавы и питоны Уход и содержание', 2009, 'Профиздат'),
    #  ('Алгоритмы неформально. Инструкция для начинающих питонистов', 2022, 'Питер'),
    #  ('Изучаем Python. Том 1', 2019, 'Диалектика'),
    #  ('Изучаем Python. Том 2', 2020, 'Вильямс'),
    #  ...
    # ]
    cur.execute("""SELECT
                      Book_Name,
                      Publication_Year as Year,
                      Publisher_Name      
                   FROM
                      Books
                      INNER JOIN Publishers ON Publisher_Id = Publishers.Id
                """)
    # Выводим результат
    result = cur.fetchall()
    print(result)

    # Делаем выборку наименований книг и авторов.
    # Формат выборки: список из кортежей, примерно так:
    # [('Удавы и питоны Уход и содержание', 'Крицкий А.'),
    #  ('Алгоритмы неформально. Инструкция для начинающих питонистов', 'Брэдфорд Такфилд'),
    # ...
    #  ('Цель на 360. Управляй судьбой', 'Пелехатый М.
    # ]
    cur.execute("""SELECT
                      Book_Name,
                      Author_Name
                   FROM
                      Books
                      INNER JOIN Books_Authors ON Books.Id = Books_Authors.Book_Id
                      INNER JOIN Authors ON Books_Authors.Author_Id = Authors.Id
                """)
    # https://www.cyberforum.ru/ms-access/thread2218485.html
    result = cur.fetchall()
    print(result)

    # Делаем выборку наименований книг и авторов.
    # Запрос немного иной, результат тот же.
    # https://www.cyberforum.ru/ms-access/thread2218485.html
    cur.execute(''' SELECT
                    Books.Book_Name,
                    Authors.Author_Name
                    FROM
                        Authors
                        INNER JOIN
                            (Books
                            INNER JOIN
                                Books_Authors
                                ON
                                    Books.Id = Books_Authors.Book_Id)
                            ON
                                Authors.Id = Books_Authors.Author_Id
                ''')
    result = cur.fetchall()
    print(result)

    # Делаем выборку наименований книг и авторов.
    # Запрос немного иной, результат тот же.
    # https://www.cyberforum.ru/delphi-database/thread2217087.html#post12251513
    cur.execute('''
    SELECT 
        Books.Book_Name, 
        Authors.Author_Name
    FROM 
        Books_Authors 
            JOIN 
                Books 
                ON 
                    Books.Id = Books_Authors.Book_Id
            JOIN 
                Authors 
                ON 
                    Authors.Id = Books_Authors.Author_Id''')
    result = cur.fetchall()
    print(result)


    # Делаем выборку наименований книг и авторов.
    # Группируем по книгам
    cur.execute(''' SELECT
                    Books.Book_Name,
                    Authors.Author_Name
                    FROM
                        Authors
                        INNER JOIN
                            (Books
                            INNER JOIN
                                Books_Authors
                                ON
                                    Books.Id = Books_Authors.Book_Id)
                            ON
                                Authors.Id = Books_Authors.Author_Id
                    GROUP BY Books.Book_Name
                ''')
    result = cur.fetchall()
    print(result)

