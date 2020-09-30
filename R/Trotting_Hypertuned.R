# This program downloads prepared horse racing data from Google sheets (for coming competition),
# Builds a hyper parameter-tuned random forest model on historical data (database sore in AWS), and makes predictions
# on the probability of winning for each horse participating in a race
# Charles Rain, 2020


if (!require('gsheet')) install.packages('gsheet')
library('gsheet')
if (!require('RPostgreSQL')) install.packages('RPostgreSQL') 
library('RPostgreSQL')
if (!require('DBI')) install.packages('DBI') 
library('DBI')
if (!require('dplyr')) install.packages('dplyr') 
library('dplyr') 
if (!require('ranger')) install.packages('ranger') 
library('ranger')
if (!require('tidymodels')) install.packages('tidymodels') 
library('tidymodels')
if (!require('dbplyr')) install.packages('dbplyr') 
library('dbplyr')
if (!require('tidyverse')) install.packages('tidyverse') 
library('tidyverse')
if (!require('svDialogs')) install.packages('svDialogs') 
library('svDialogs')
if (!require('vip')) install.packages('vip')
library(vip)
if (!require('themis')) install.packages('themis') 
library(themis)
if (!require('doParallel')) install.packages('doParallel') 
library(doParallel)

#declarations
pred <- NULL #dataset we use our model to make predictions on
train <- NULL #historical data used for training
final <- NULL #not used
modlist <- list() #final list of per race models
figures <- list() #VIP charts for each model
fits <- list() #stored list of final per race models


#prompt for AWS user credentials
AWSuser <- dlgInput("Enter AWS user:", Sys.info()["user"])$res
AWSpassword <- dlgInput("Enter AWS password:", Sys.info()["user"])$res #


#declare variables & read prepared google sheet into data frame
urls <-
  c(
    'https://docs.google.com/spreadsheets/d/1RIjiQVboF1D_Sddy5ZbKbfi-MjetxQ6SX2ERyHQEr68/edit#gid=0',
    'https://docs.google.com/spreadsheets/d/1RIjiQVboF1D_Sddy5ZbKbfi-MjetxQ6SX2ERyHQEr68/edit#gid=436433626',
    'https://docs.google.com/spreadsheets/d/1RIjiQVboF1D_Sddy5ZbKbfi-MjetxQ6SX2ERyHQEr68/edit#gid=1184555443',
    'https://docs.google.com/spreadsheets/d/1RIjiQVboF1D_Sddy5ZbKbfi-MjetxQ6SX2ERyHQEr68/edit#gid=330887737',
    'https://docs.google.com/spreadsheets/d/1RIjiQVboF1D_Sddy5ZbKbfi-MjetxQ6SX2ERyHQEr68/edit#gid=716133410',
    'https://docs.google.com/spreadsheets/d/1RIjiQVboF1D_Sddy5ZbKbfi-MjetxQ6SX2ERyHQEr68/edit#gid=910016978',
    'https://docs.google.com/spreadsheets/d/1RIjiQVboF1D_Sddy5ZbKbfi-MjetxQ6SX2ERyHQEr68/edit#gid=265580164'
  )


Overview <-
  gsheet2tbl(
    'https://docs.google.com/spreadsheets/d/1RIjiQVboF1D_Sddy5ZbKbfi-MjetxQ6SX2ERyHQEr68/edit#gid=235888937'
  )

for (i in 1:length(urls)) 
{
  imp <- gsheet2tbl(urls[i])
  pred <-
    rbind(pred,
          cbind(
            imp[, c(1:16)] 
           
          ))
}


#connect to AWS
con <-
  dbConnect(
    RPostgreSQL::PostgreSQL(),
    dbname = "postgres",
    host = "database-1.ctlrikt10pos.ca-central-1.rds.amazonaws.com",
    user = AWSuser,
    password = AWSpassword
  )


#set default schema for session
dbSendQuery(con, "set search_path to public")


#Create data set for use in modeling by querying AWS
for (i in 1:nrow(Overview))
{
  query <-
    build_sql(
      "select framspar, won, /*a.trackno,*/ ", as.character(i) ," as raceno, a.betperc, a.moneyrank, a.winsperc,/* a.winodds,*/ a.pointsperc, a.winperccurrent, a.placep, a.jockeyrank, a.trainerwinperc from V75flat a where a.division = ",
      as.character(Overview[i, 2]) ,
      "",
      " and a.distans = ",
      as.character(Overview[i, 3]),
      "",
      " and a.startsatt = ",
      as.character(Overview[i, 4]),
      
      "and a.trackno != 0;",
      con = con)
  
  train <- rbind(train, dbGetQuery(con, query))
  
}

predictions <- train[0,] %>% select(-won)


#clean data
oldraces <- train %>% mutate_if(is.numeric, list(~replace_na(., 0))) %>% 
  mutate_if(~(length(unique(.)) ==1), rm)


#ensure outcome variable is factor
oldraces$won <- as.factor(oldraces$won)


#enable parallel processing
all_cores <- parallel::detectCores(logical = FALSE)
cl <- makePSOCKcluster(all_cores)
registerDoParallel(cl)


#loop over Race subsets, creating hyperparameter-tuned random forest model, and make predictions
for (i in 1:nrow(Overview))
{
  #create subset
  lopp <- oldraces %>% filter(raceno==i)
  
  #create training and testing sets
  lopp_split <- initial_split(lopp, strata = won) 
  lopp_train <- training(lopp_split)
  lopp_test <- testing(lopp_split)
  
  lopp_rec <- recipe(won ~ ., data = lopp) %>% step_upsample(won, over_ratio = 1)
  
  lopp_prep <- prep(lopp_rec) #prepare recipe
  juiced <- juice(lopp_prep) #apply recipe
  
  #specify model and engine
  lopp_spec <- rand_forest(
    mtry = tune(), #hyperparameter to be tuned
    trees = 200,
    min_n = tune() #hyperparameter to be tuned
  ) %>%
    set_mode("classification") %>%
    set_engine("ranger")
  
  #create workflow
  lopp_wf <- workflow() %>%
    add_recipe(lopp_rec) %>%
    add_model(lopp_spec)
  
  #prepare cross-validation used in tuning hyperparameters
  lopp_folds <- vfold_cv(lopp_train)
  
  #enable paralell processing
  all_cores <- parallel::detectCores(logical = FALSE)
  cl <- makePSOCKcluster(all_cores)
  registerDoParallel(cl)
  
  lopp_res <- tune_grid(
    lopp_wf,
    resamples = lopp_folds,
    grid = 20 #or spline_grid from below
  )
  
  #print hyperparameter tuning plot
  lopp_res %>%
    collect_metrics() %>%
    filter(.metric == "roc_auc") %>%
    select(mean, min_n, mtry) %>%
    pivot_longer(min_n:mtry,
                 values_to = "value",
                 names_to = "parameter"
    ) %>%
    ggplot(aes(value, mean, color = parameter)) +
    geom_point(show.legend = FALSE) +
    facet_wrap(~parameter, scales = "free_x") +
    labs(x = NULL, y = "AUC")
  
  #chose best option from cov_res and regular_res
  best_auc <- select_best(lopp_res, "roc_auc") 
  
  #finalize model
  final_rf <- finalize_model(
    lopp_spec,
    best_auc
  )
  
  figures[i] <- final_rf %>%
    set_engine("ranger", importance = "permutation") %>%
    fit(won ~ ., #use same formula as in final recipe
        data = juice(lopp_prep)
    ) %>%
    vip(geom = "point")
  

  
  #prepare fitted model for forecasting
  finalfit <- final_rf %>%
    set_engine("ranger", importance = "permutation") %>%
    fit(won ~ ., #use same formula as in final recipe
        data = juice(lopp_prep))
  
  fits[[i]] <- finalfit
  
  #prep, test and print final model performance metrics
  final_wf <- workflow() %>%
    add_recipe(lopp_rec) %>%
    add_model(final_rf)
  
  final_res <- final_wf %>%
    last_fit(lopp_split)
  
  modlist[[i]] <- final_res #save model objects in list
  
  final_res %>%
    collect_metrics()
  
  predictions <- rbind(predictions, cbind(filter(pred, raceno==i), predict(finalfit,filter(pred, raceno==i), type = "prob")))
  
}






            