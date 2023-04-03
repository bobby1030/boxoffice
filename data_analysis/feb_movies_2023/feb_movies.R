library(tidyverse)
library(dbplyr)
library(lubridate)
library(ggthemes)

box_db_conn <- DBI::dbConnect(RSQLite::SQLite(), "boxoffice.db")
box_db <- tbl(box_db_conn, "BoxOfficeDaily")

movies <- box_db %>% 
    filter(name == "日麗" | name == "親密" | name == "灌籃高手 THE FIRST SLAM DUNK" | name == "關於我和鬼變成家人的那件事") %>% 
    collect()

DBI::dbDisconnect(box_db_conn)

movies_frame <- movies %>% 
    select(date, name, theaterCount, tickets, totalTickets, amounts, totalAmounts, ongoingDays) %>% 
    arrange(date) %>% 
    mutate(
        date = as.Date(date, "%Y-%m-%d"),
        dayOfWeek = wday(date, label = TRUE, locale = "zh_TW.UTF-8"),
        dateAbbr = paste(format(date, "%m/%d"), dayOfWeek, sep = "\n"),

        ticketsPerTheater = tickets / theaterCount,
        amountsPerTheater = amounts / theaterCount,
    ) %>% 
    filter(
        case_when(
            name == "灌籃高手 THE FIRST SLAM DUNK" ~ date >= "2022-01-01",
            name == "日麗" ~ date >= "2023-02-10",
            name == "親密" ~ date >= "2023-02-17" & date <= "2023-03-20",
            name == "關於我和鬼變成家人的那件事" ~ TRUE,
            TRUE ~ FALSE
        )
    )

feb_movies <- ggplot(movies_frame, aes(x = date, y = amountsPerTheater, color = name)) +
    geom_path() +
    geom_point(data = ~ .x[.x$dayOfWeek %in% c("六", "日"), ], aes(shape = dayOfWeek), size = 2.5) +
    labs(title = "每家上映戲院平均票房", x = "日期", y = "每戲院平均票房", color = "片名", shape = "週末") +
    scale_x_date(date_breaks = "1 week", date_labels = "%m/%d") +
    scale_y_log10(labels = scales::comma) +
    theme_fivethirtyeight()
plot(feb_movies)
ggsave("./data_analysis/feb_movies_2023/feb_movies.png", feb_movies, width = 10, height = 6, dpi = 300, type = "cairo-png")
