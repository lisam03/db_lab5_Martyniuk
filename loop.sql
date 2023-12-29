-- Додавання Гран Прі з 1111 по 1115
DO $$ 
DECLARE
    i INT := 1111;
BEGIN
    LOOP
        EXIT WHEN i > 1115; -- Зупинка після вставки 5 рядків
        INSERT INTO races (race_id, race_year,  race_name) VALUES (i, 2023, i || ' Grand Prix');
        i := i + 1;
    END LOOP;
END $$;
  
-- Перевірка результату
select * from races;

-- Видалення доданих рядків
DELETE FROM races WHERE race_id = 1111;
DELETE FROM races WHERE race_id = 1112;
DELETE FROM races WHERE race_id = 1113;
DELETE FROM races WHERE race_id = 1114;
DELETE FROM races WHERE race_id = 1115;