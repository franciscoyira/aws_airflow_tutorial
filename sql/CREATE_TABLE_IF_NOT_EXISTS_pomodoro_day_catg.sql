CREATE TABLE IF NOT EXISTS pomodoro_day_catg (
  date DATE NOT NULL,
  learning_minutes NUMERIC NOT NULL,
  work_minutes NUMERIC NOT NULL,
  CONSTRAINT date_unique UNIQUE (date)
);