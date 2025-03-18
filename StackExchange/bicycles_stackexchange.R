Users <- read.csv("Users.csv")
Votes <- read.csv("Votes.csv")
Tags <- read.csv("Tags.csv")
PostLinks <- read.csv("PostLinks.csv")
Comments <- read.csv("Comments.csv")
Badges <- read.csv("Badges.csv")
Posts <- read.csv("Posts.csv")

library(dplyr)
library(sqldf)
library(compare)
library(microbenchmark)

################################################################################
################################################################################

############################
# TASK 1: BASE R
############################

questions_users_ids <- merge(na.omit(Posts[Posts$PostTypeId == 1, c('Id', "OwnerUserId")]), Users["Id"],
              by.x = "OwnerUserId", by.y = 'Id')
questions_counts <- aggregate(questions_users_ids, Id ~ OwnerUserId, length)
names(questions_counts)[2] <- "QuestionsTotal"

answers_users_ids <- merge(na.omit(Posts[Posts$PostTypeId == 2, c('Id', "OwnerUserId")]), Users["Id"],
                             by.x = "OwnerUserId", by.y = 'Id')
answers_counts <- aggregate(answers_users_ids$Id, answers_users_ids["OwnerUserId"], length)
names(answers_counts)[2] <- "AnswersTotal"

questions_users_ids <- merge(na.omit(Posts[Posts$PostTypeId == 1, c('Id', "OwnerUserId")]), Users["Id"],
                             by.x = "OwnerUserId", by.y = 'Id')
questions_counts <- aggregate(questions_users_ids$Id, questions_users_ids["OwnerUserId"], length)
names(questions_counts)[2] <- "QuestionsTotal"

comments_users_ids <- merge(na.omit(Comments[c('Id', "UserId")]), Users["Id"],
                             by.x = "UserId", by.y = 'Id')
comments_counts <- aggregate(comments_users_ids$Id, comments_users_ids["UserId"], length)
names(comments_counts)[2] <- "CommentsTotal"

# can have NA in AcceptedAnswerId, so na.omit();
# merge works only with dataframes, so casting
answer_ids <- na.omit(data.frame(AcceptedAnswerId = Posts[Posts$PostTypeId == 1, "AcceptedAnswerId"]))

accepted_users_ids <- merge(na.omit(Posts[Posts$PostTypeId == 2, c("Id", "OwnerUserId")]),
      answer_ids,
      by.x = 'Id', by.y = 'AcceptedAnswerId')

accepted_counts <- aggregate(accepted_users_ids$Id, accepted_users_ids["OwnerUserId"], length)
colnames(accepted_counts)[2] <- "AcceptedAnswerCount"

users_stats <- merge(Users[c("Id", "DisplayName", "UpVotes", "DownVotes")], accepted_counts,
                    by.x = "Id", by.y = "OwnerUserId")

users_stats <- merge(users_stats, questions_counts, by.x = "Id", by.y = "OwnerUserId", all.x = TRUE)
users_stats <- merge(users_stats, answers_counts, by.x = "Id", by.y = "OwnerUserId", all.x = TRUE)
users_stats <- merge(users_stats, comments_counts, by.x = "Id", by.y = "UserId", all.x = TRUE)

users_stats$QuestionsTotal[is.na(users_stats$QuestionsTotal)] <- 0
users_stats$AnswersTotal[is.na(users_stats$AnswersTotal)] <- 0
users_stats$CommentsTotal[is.na(users_stats$CommentsTotal)] <- 0

users_stats$Score <- users_stats$UpVotes - users_stats$DownVotes
users_stats <- users_stats[, -c(3,4)]

task1_base <- head(users_stats[order(-users_stats$AcceptedAnswerCount), ], 10)

head(task1_base, 10)
#> head(task1_base, 10)
#Id       DisplayName AcceptedAnswerCount QuestionsTotal AnswersTotal CommentsTotal Score
#933  25959    Nathan Knutson                 632             15         1556          1385  1094
#790  19705           Criggie                 563             58         3117         14173 24359
#898  24228 Argenti Apparatus                 452              3         1685          2221  1676
#554   8219            Batman                 321              7          873          3432  2064
#336   3924            mattnz                 250              9          851          3045  3254
#522   7309           Chris H                 203             90          930          6552  1808
#195   1584    Daniel R Hicks                 198             16         1025         11828  1687
#160   1259           zenbike                 196             10          465          1187   704
#1172 38270         Weiwen Ng                 150              4          667          2053  5081
#366   4534           Rider_X                 148              5          358          1268   489


############################
# TASK 1: DPLYR
############################

questions_users_ids <- Posts %>%
  filter(PostTypeId == 1) %>%
  inner_join(Users, by = c("OwnerUserId" = "Id")) %>%
  select(OwnerUserId, Id) %>% na.omit()

answers_users_ids <- Posts %>%
  filter(PostTypeId == 2) %>%
  inner_join(Users, by = c("OwnerUserId" = "Id")) %>%
  select(OwnerUserId, Id) %>% na.omit()

comments_users_ids <- Comments %>%
  inner_join(Users, by = c("UserId" = "Id")) %>%
  select(UserId, Id) %>% na.omit()

questions_counts <- questions_users_ids %>% group_by(OwnerUserId) %>%
  summarise(QuestionsTotal = n())

answers_counts <- answers_users_ids %>% group_by(OwnerUserId) %>%
  summarise(AnswersTotal = n())

comments_counts <- comments_users_ids %>% group_by(UserId) %>%
  summarise(CommentsTotal = n())

answer_ids <- Posts %>%
  filter(PostTypeId == 1) %>% 
  select(AcceptedAnswerId) %>%
  na.omit()

accepted_users_ids <- Posts %>%
  filter(PostTypeId == 2) %>%
  inner_join(answer_ids, by = c("Id" = 'AcceptedAnswerId')) %>%
  select(Id, OwnerUserId)

accepted_counts <- accepted_users_ids %>% group_by(OwnerUserId) %>%
  summarise(AcceptedAnswerCount = n())

users_stats <- Users %>%
  inner_join(accepted_counts, by = c("Id" = "OwnerUserId")) %>%
  select(Id, DisplayName, AcceptedAnswerCount, UpVotes, DownVotes) %>%
  na.omit()

users_stats <- users_stats %>%
  left_join(questions_counts, by = c("Id" = "OwnerUserId")) %>%
  left_join(answers_counts, by = c("Id" = "OwnerUserId")) %>%
  left_join(comments_counts, by = c("Id" = "UserId"))

# rewriting all NA values with 0 with coalense()
users_stats <- users_stats %>%
  mutate(
    QuestionsTotal = coalesce(QuestionsTotal, 0),
    AnswersTotal = coalesce(AnswersTotal, 0),
    CommentsTotal = coalesce(CommentsTotal, 0)
  )

users_stats <- users_stats %>%
  mutate(Score = UpVotes - DownVotes)

users_stats <- users_stats %>%
  select(-c(4, 5))

task1_dplyr <- users_stats %>%
  arrange(desc(AcceptedAnswerCount)) %>%
  slice_head(n = 10)

head(task1_dplyr, 10)
#> head(task1_dplyr, 10)
#Id       DisplayName AcceptedAnswerCount QuestionsTotal AnswersTotal CommentsTotal Score
#1  25959    Nathan Knutson                 632             15         1556          1385  1094
#2  19705           Criggie                 563             58         3117         14173 24359
#3  24228 Argenti Apparatus                 452              3         1685          2221  1676
#4   8219            Batman                 321              7          873          3432  2064
#5   3924            mattnz                 250              9          851          3045  3254
#6   7309           Chris H                 203             90          930          6552  1808
#7   1584    Daniel R Hicks                 198             16         1025         11828  1687
#8   1259           zenbike                 196             10          465          1187   704
#9  38270         Weiwen Ng                 150              4          667          2053  5081
#10  4534           Rider_X                 148              5          358          1268   489


############################
# TASK 1: COMPARE() AND MICROBENCHMARK()
############################

compare(task1_dplyr, task1_base, allowAll = TRUE)
#> compare(task1_dplyr, task1_base, allowAll = TRUE)
#TRUE
#renamed rows
#dropped row names


microbenchmark_task1 <- microbenchmark(
  base = {
    questions_users_ids <- merge(na.omit(Posts[Posts$PostTypeId == 1, c('Id', "OwnerUserId")]), Users["Id"],
                                 by.x = "OwnerUserId", by.y = 'Id')
    questions_counts <- aggregate(questions_users_ids, Id ~ OwnerUserId, length)
    names(questions_counts)[2] <- "QuestionsTotal"
    
    answers_users_ids <- merge(na.omit(Posts[Posts$PostTypeId == 2, c('Id', "OwnerUserId")]), Users["Id"],
                               by.x = "OwnerUserId", by.y = 'Id')
    answers_counts <- aggregate(answers_users_ids$Id, answers_users_ids["OwnerUserId"], length)
    names(answers_counts)[2] <- "AnswersTotal"
    
    questions_users_ids <- merge(na.omit(Posts[Posts$PostTypeId == 1, c('Id', "OwnerUserId")]), Users["Id"],
                                 by.x = "OwnerUserId", by.y = 'Id')
    questions_counts <- aggregate(questions_users_ids$Id, questions_users_ids["OwnerUserId"], length)
    names(questions_counts)[2] <- "QuestionsTotal"
    
    comments_users_ids <- merge(na.omit(Comments[c('Id', "UserId")]), Users["Id"],
                                by.x = "UserId", by.y = 'Id')
    comments_counts <- aggregate(comments_users_ids$Id, comments_users_ids["UserId"], length)
    names(comments_counts)[2] <- "CommentsTotal"
    
    # can have NA AcceptedAnswerId, so na.omit();
    # merge works only with dataframes, so casting
    answer_ids <- na.omit(data.frame(AcceptedAnswerId = Posts[Posts$PostTypeId == 1, "AcceptedAnswerId"]))
    
    accepted_users_ids <- merge(na.omit(Posts[Posts$PostTypeId == 2, c("Id", "OwnerUserId")]),
                                answer_ids,
                                by.x = 'Id', by.y = 'AcceptedAnswerId')
    
    accepted_counts <- aggregate(accepted_users_ids$Id, accepted_users_ids["OwnerUserId"], length)
    colnames(accepted_counts)[2] <- "AcceptedAnswerCount"
    
    users_stats <- merge(Users[c("Id", "DisplayName", "UpVotes", "DownVotes")], accepted_counts,
                         by.x = "Id", by.y = "OwnerUserId")
    
    users_stats <- merge(users_stats, questions_counts, by.x = "Id", by.y = "OwnerUserId", all.x = TRUE)
    users_stats <- merge(users_stats, answers_counts, by.x = "Id", by.y = "OwnerUserId", all.x = TRUE)
    users_stats <- merge(users_stats, comments_counts, by.x = "Id", by.y = "UserId", all.x = TRUE)
    
    users_stats$QuestionsTotal[is.na(users_stats$QuestionsTotal)] <- 0
    users_stats$AnswersTotal[is.na(users_stats$AnswersTotal)] <- 0
    users_stats$CommentsTotal[is.na(users_stats$CommentsTotal)] <- 0
    
    users_stats$Score <- users_stats$UpVotes - users_stats$DownVotes
    users_stats <- users_stats[, -c(3,4)]
    
    task1_base <- head(users_stats[order(-users_stats$AcceptedAnswerCount), ], 10)
  },
  dplyr = {
    questions_users_ids <- Posts %>%
      filter(PostTypeId == 1) %>%
      inner_join(Users, by = c("OwnerUserId" = "Id")) %>%
      select(OwnerUserId, Id) %>% na.omit()
    
    answers_users_ids <- Posts %>%
      filter(PostTypeId == 2) %>%
      inner_join(Users, by = c("OwnerUserId" = "Id")) %>%
      select(OwnerUserId, Id) %>% na.omit()
    
    comments_users_ids <- Comments %>%
      inner_join(Users, by = c("UserId" = "Id")) %>%
      select(UserId, Id) %>% na.omit()
    
    questions_counts <- questions_users_ids %>% group_by(OwnerUserId) %>%
      summarise(QuestionsTotal = n())
    
    answers_counts <- answers_users_ids %>% group_by(OwnerUserId) %>%
      summarise(AnswersTotal = n())
    
    comments_counts <- comments_users_ids %>% group_by(UserId) %>%
      summarise(CommentsTotal = n())
    
    answer_ids <- Posts %>%
      filter(PostTypeId == 1) %>% 
      select(AcceptedAnswerId) %>%
      na.omit()
    
    accepted_users_ids <- Posts %>%
      filter(PostTypeId == 2) %>%
      inner_join(answer_ids, by = c("Id" = 'AcceptedAnswerId')) %>%
      select(Id, OwnerUserId)
    
    accepted_counts <- accepted_users_ids %>% group_by(OwnerUserId) %>%
      summarise(AcceptedAnswerCount = n())
    
    users_stats <- Users %>%
      inner_join(accepted_counts, by = c("Id" = "OwnerUserId")) %>%
      select(Id, DisplayName, AcceptedAnswerCount, UpVotes, DownVotes) %>%
      na.omit()
    
    users_stats <- users_stats %>%
      left_join(questions_counts, by = c("Id" = "OwnerUserId")) %>%
      left_join(answers_counts, by = c("Id" = "OwnerUserId")) %>%
      left_join(comments_counts, by = c("Id" = "UserId"))
    
    users_stats <- users_stats %>%
      mutate(
        QuestionsTotal = coalesce(QuestionsTotal, 0),
        AnswersTotal = coalesce(AnswersTotal, 0),
        CommentsTotal = coalesce(CommentsTotal, 0)
      )
    
    users_stats <- users_stats %>%
      mutate(Score = UpVotes - DownVotes)
    
    users_stats <- users_stats %>%
      select(-c(4, 5))
    
    task1_dplyr <- users_stats %>%
      arrange(desc(AcceptedAnswerCount)) %>%
      slice_head(n = 10)
  },
  times = 25)

print(microbenchmark_task1)
#> print(microbenchmark_task1)
#Unit: milliseconds
#expr       min        lq      mean    median        uq      max neval
#base 1224.6840 1312.0954 1468.4899 1442.4297 1554.5670 1924.708    25
#dplyr  541.4628  721.1941  803.5961  817.9801  868.9899 1156.658    25


################################################################################
################################################################################

############################
# TASK 2: BASE R
############################

desired_badges <- c("Teacher", "Explainer", "Commentator")

badges_filtered <- subset(Badges, Name %in% desired_badges)

users_with_badges <- na.omit(merge(badges_filtered[c("UserId", "Name")], Users[c("Id", "Location")],
                                   by.x = "UserId", by.y = 'Id'))

# not only cleaning NA, but some "empty" locations I have found
users_with_badges <- subset(users_with_badges, Location != "" & Location != "  ")

badges_counts <- aggregate(users_with_badges$UserId, users_with_badges["Location"], length)
names(badges_counts)[2] <- "BadgesTotal"

posts_badges <- merge(Posts[c("Id", "OwnerUserId")], users_with_badges,
                      by.x = "OwnerUserId", by.y = 'UserId')

# posts from the user with 2+ badge will duplicate, removing them
posts_badges <- posts_badges[!duplicated(posts_badges[c("Id", "OwnerUserId")]), ]

posts_by_location <- aggregate(posts_badges$Id, posts_badges["Location"], length)
names(posts_by_location)[2] <- "PostsTotal"

task2_base <- merge(badges_counts, posts_by_location, by = "Location")

task2_base <- task2_base[order(-task2_base$PostsTotal, -task2_base$BadgesTotal), ]

head(task2_base)
#> head(task2_base)
#Location BadgesTotal PostsTotal
#691       New Zealand          13       3337
#1086    Washington DC           5       1707
#1033               UK          43       1631
#903  Seattle, WA, USA           5       1596
#1036    United States          65       1122
#635    Minnesota, USA           3       1041


############################
# TASK 2: DPLYR
############################

desired_badges <- c("Teacher", "Explainer", "Commentator")

users_with_badges <- Badges %>%
  filter(Name %in% desired_badges) %>%
  inner_join(Users, by = c("UserId" = "Id")) %>%
  select(UserId, Location, Name) %>%
  na.omit() %>% filter(Location != "" & Location != "  ")

badges_counts <- users_with_badges %>% group_by(Location) %>%
  summarise(BadgesTotal = n())

posts_by_location <- Posts %>%
  inner_join(users_with_badges, by = c("OwnerUserId" = "UserId"), relationship = "many-to-many") %>%
  distinct(Id, OwnerUserId, Location) %>%
  group_by(Location) %>%
  summarise(PostsTotal = n())

task2_dplyr <- badges_counts %>%
  inner_join(posts_by_location, by = "Location") %>%
  arrange(desc(PostsTotal), desc(BadgesTotal)) %>% as.data.frame()

head(task2_dplyr)
#> head(task2_dplyr)
#Location BadgesTotal PostsTotal
#1      New Zealand          13       3337
#2    Washington DC           5       1707
#3               UK          43       1631
#4 Seattle, WA, USA           5       1596
#5    United States          65       1122
#6   Minnesota, USA           3       1041


############################
# TASK 2: COMPARE() AND MICROBENCHMARK()
############################

compare(task2_dplyr, task2_base, allowAll = TRUE)
#> compare(task2_dplyr, task2_base, allowAll = TRUE)
#TRUE
#sorted
#renamed rows
#dropped row names


microbenchmark_task2 <- microbenchmark(
  base = {
    desired_badges <- c("Teacher", "Explainer", "Commentator")
    
    badges_filtered <- subset(Badges, Name %in% desired_badges)
    
    users_with_badges <- na.omit(merge(badges_filtered[c("UserId", "Name")], Users[c("Id", "Location")],
                                       by.x = "UserId", by.y = 'Id'))
    
    users_with_badges <- subset(users_with_badges, Location != "" & Location != "  ")
    
    badges_counts <- aggregate(users_with_badges$UserId, users_with_badges["Location"], length)
    names(badges_counts)[2] <- "BadgesTotal"
    
    posts_badges <- merge(Posts[c("Id", "OwnerUserId")], users_with_badges,
                          by.x = "OwnerUserId", by.y = 'UserId')
    
    # posts from the user with 2+ badge will duplicate, removing them
    posts_badges <- posts_badges[!duplicated(posts_badges[c("Id", "OwnerUserId")]), ]
    
    posts_by_location <- aggregate(posts_badges$Id, posts_badges["Location"], length)
    names(posts_by_location)[2] <- "PostsTotal"
    
    task2_base <- merge(badges_counts, posts_by_location, by = "Location")
    
    task2_base <- task2_base[order(-task2_base$PostsTotal, -task2_base$BadgesTotal), ]
  },
  dplyr = {
    desired_badges <- c("Teacher", "Explainer", "Commentator")
    
    users_with_badges <- Badges %>%
      filter(Name %in% desired_badges) %>%
      inner_join(Users, by = c("UserId" = "Id")) %>%
      select(UserId, Location, Name) %>%
      na.omit() %>% filter(Location != "" & Location != "  ")
    
    badges_counts <- users_with_badges %>% group_by(Location) %>%
      summarise(BadgesTotal = n())
    
    posts_by_location <- Posts %>%
      inner_join(users_with_badges, by = c("OwnerUserId" = "UserId"), relationship = "many-to-many") %>%
      distinct(Id, OwnerUserId, Location) %>%
      group_by(Location) %>%
      summarise(PostsTotal = n())
    
    task2_dplyr <- badges_counts %>%
      inner_join(posts_by_location, by = "Location") %>%
      arrange(desc(PostsTotal), desc(BadgesTotal)) %>% as.data.frame()
  },
  times = 25)

print(microbenchmark_task2)
#> print(microbenchmark_task2)
#Unit: milliseconds
#expr      min       lq     mean   median       uq       max neval
#base 442.6068 551.4911 735.5855 634.0297 897.0492 1242.8209    25
#dplyr  81.5829  91.0741 135.2468 133.2687 159.7880  264.3518    25

################################################################################
################################################################################

############################
# TASK 3: SQLDF AND EXPLANATION
############################

task3_sql <- sqldf("SELECT
Posts.Title,
UpVotesPerYear.Year,
MAX(UpVotesPerYear.Count) AS Count
FROM (
  SELECT
  PostId,
  COUNT(*) AS Count,
  STRFTIME('%Y', Votes.CreationDate) AS Year
  FROM Votes
  WHERE VoteTypeId=2
  GROUP BY PostId, Year
) AS UpVotesPerYear
JOIN Posts ON Posts.Id=UpVotesPerYear.PostId
WHERE Posts.PostTypeId=1
GROUP BY Year
ORDER BY Year ASC")

# Explanation: the query result is a table showing the title of the most
# upvoted question each year, the year, and the upvote count.


############################
# TASK 3: DPLYR
############################

UpVotesPerYear <- Votes %>%
  filter(VoteTypeId == 2) %>%
  mutate(Year = format(as.Date(CreationDate), "%Y")) %>%
  group_by(PostId, Year) %>%
  summarise(Count = n(), .groups = "drop")

task3_dplyr <- UpVotesPerYear %>%
  inner_join(Posts, by = c("PostId" = "Id")) %>%
  filter(PostTypeId == 1) %>%
  group_by(Year) %>%
  summarise(
    Title = Title[which.max(Count)],
    Count = max(Count)
  ) %>% as.data.frame()

head(task3_dplyr)
#> head(task3_dplyr)
#Year                                                 Title Count
#1 2010                           Why ride a fixed-gear bike?    21
#2 2011                           Why ride a fixed-gear bike?    31
#3 2012 How do I deal with a dog chasing me when I'm touring?    41
#4 2013                 Ride with someone who is less trained    34
#5 2014        How to get over anger at inconsiderate drivers    33
#6 2015      How to commute to work on your bike and dress up    39


############################
# TASK 3: COMPARE()
############################

compare(task3_sql, task3_dplyr, allowAll = TRUE)
#> compare(task3_sql, task3_dplyr, allowAll = TRUE)
#TRUE 
#reordered columns