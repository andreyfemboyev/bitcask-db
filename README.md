# Bitcask DB

**An embedded persistent key-value engine written in Python 3.12 using only the standard library.** Designed for local Linux filesystems; includes append-only segments, CRC32 checks, recovery and compaction.

[RaftDB Extended](https://github.com/andreyfemboyev/raftfb_extended) adapts ideas from this storage design. [Project portfolio](https://github.com/andreyfemboyev/portfolio).

| Tool | Best suited to |
| --- | --- |
| Python dict | In-memory key-value access without built-in persistence |
| Bitcask DB | Studying durable append-only key-value storage |
| SQLite | Embedded relational queries and transactions |

```mermaid
flowchart LR
    A[21-byte header] --> B[Key bytes] --> C[Value bytes]
```

## Документация на русском

Встраиваемая дисковая база «ключ–значение». Реализация использует стандартную библиотеку Python 3.12 и рассчитана на Linux с локальной файловой системой.

## Командная строка

Запустите из каталога проекта.

```sh
python3 bitcask.py --dir ./demo put цвет синий
python3 bitcask.py --dir ./demo get цвет
python3 bitcask.py --dir ./demo delete цвет
python3 bitcask.py --dir ./demo compact
```

CLI кодирует аргументы в UTF-8. Команда `get` выводит байты значения без добавления перевода строки. Код выхода `0` означает успех, `1` означает отсутствие ключа, `2` означает ошибку. Каталог базы создаётся автоматически. Используйте для базы отдельный каталог.

## API

```python
from bitcask import Bitcask

with Bitcask("./demo", segment_size=16 * 1024 * 1024) as db:
    db.put(b"user:42", b"Ada")
    assert db.get(b"user:42") == b"Ada"
    assert db.delete(b"user:42") is True
    assert db.get(b"user:42") is None
    db.compact()
```

Ключи и значения имеют тип `bytes`. Ключ занимает от 1 байта до 1 МиБ, значение может быть пустым и занимает до 64 МиБ. Размер сегмента по умолчанию равен 16 МиБ. Если одна запись превышает этот порог, база выделяет ей отдельный сегмент.

## Хранение и восстановление

- `put` и `delete` дописывают записи в конец активного сегмента и вызывают `fsync` перед возвратом. Удаление существующего ключа записывает маркер удаления. Заполненный сегмент становится неизменяемым.
- Индекс в памяти сопоставляет ключ с номером сегмента и смещением. `get` читает запись по этому адресу и проверяет CRC32. При открытии база последовательно проигрывает журнал, чтобы восстановить индекс.
- Неполная последняя запись активного сегмента отбрасывается при открытии. При несовпадении контрольной суммы или неполной записи в архивном сегменте база выдаёт ошибку повреждения.
- `compact` копирует актуальные записи в отдельное поколение, синхронизирует файлы и атомарно заменяет указатель `CURRENT`. Старое поколение удаляется после синхронизации указателя. Для компактации нужно временное место под копию актуальных записей.
- Один экземпляр базы удерживает эксклюзивную блокировку каталога. Операции из разных потоков выполняются последовательно, включая компактацию. После ошибки записи с неопределённым результатом закройте и повторно откройте базу, чтобы восстановить индекс.

Каждая запись начинается с 21-байтового заголовка в порядке байтов big-endian. За ним следуют байты ключа и значения. Заголовок хранит сигнатуру `BC01`, тип операции, длины ключа и значения, CRC32 содержимого и CRC32 остальной части заголовка. Последовательность записей определяется номером сегмента и смещением.

## Тесты

```sh
python3 -m unittest discover -s tests -v
```
