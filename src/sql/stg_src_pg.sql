SELECT id, object_id, sent_dttm, payload
FROM public.messages
WHERE sent_dttm > %(date_from)s --Пропускаем те объекты, которые уже загрузили.
and object_type = %(object_type)s
ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
LIMIT %(limit)s OFFSET %(threshold)s; --Обрабатываем только одну пачку объектов.