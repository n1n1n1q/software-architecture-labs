# HW 4
Я обрав варіант з Kafka.  

## Інсталяція 

Аби підняти сервіси, слід використати docker compose.  

```
docker compose up --build -d
```  

## Демонстрація  
### Коректність білду
Спочатку, я збілдив докер, і перевірив, що всі контейнери працюють:  

![](assets/containers_ps.png)  

*через CLI*  


![](assets/containers_gui.png)  

*та через GUI*  


Опісля, можна подивитися на базові endpoints:  

![](assets/config.png)   

![](assets/services.png)  

![](assets/services2.png)  

### Коректність роботи черги  

Далі, я надіслав кілька POST-запитів з `test.http`:   

![](assets/msg1_sent.png)  


![](assets/msg5_sent.png)  


![](assets/msg10_sent.png)  

(приклад успішно надісланих реквесті, можна побачити, що кожен має різні пари offsets та partitions)  

Перевіримо загально акаунти:  

![](assets/accounts.png)  

Також, можна побачити в логах, що різні ноди отримують різні порції даних (є трохи запитів з наступної частини завдання, проте це не заважає демонстрації роботи черги):  
- [`./assets/logging_logs_pause.txt`](assets/logging_logs_pause.txt)  
- [`./assets/counter_logs.txt`](assets/counter_logs.txt)  

### Відмовостійкість  

Тепер слід перевірити відмовостійкість у кейсах, коли падає counter.  

Спочатку, через `docker pause` я зупини counter:  

![](assets/pause.png)  

Опісля, я зробив три нові транзакції:  

![](assets/msg1_pause.png)  

![](assets/msg2_pause.png)  

![](assets/msg3_pause.png)  

Можемо перевірити конкретні рахунки:  

![](assets/msg1_check_pause.png)  

![](assets/msg2_check_pause.png)  

![](assets/msg3_check_pause.png)  

Перевіримо всі рахунки:  

![](assets/accounts_pause.png)  

Можн апобачити, що баланси є null, оскільки counter впав.

Можна перевірити стан Kafka: 

![](assets/kafka_check_pause.png)  

Як видно з логів, зараз є lags, що свідчить про те, що меседжі ще не дійшли.  

Тепер, розморозимо counter service.  

Логи можна побачити у наступному файлі -- [](assets/unpause_logs.txt).  

Також, перевіримо Kafka:  

![](assets/kafka_check_unpause.png)  

Як можна побачити, зараз немає lag. 

Перевіримо тепер GET зі всіма балансами: 

![](assets/accounts_unpause.png)  

Можна побачити, що транзакції дійшли, і тепер все добре! 