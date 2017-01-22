library(shiny)
library(highcharter)
library(xts)
library(forecast)
library(dplyr)
library(bigQueryR)

## change these to your settings
YOUR_PROJECT_ID <- "projectID"
YOUR_DATASET_ID <- "datasetId"
BIGQUERY_FIELDS <- c("pageURL","Referrer","ts")

do_bq <- function(limit){

  ## authenticate offline first, upload the .httr-oauth token
  bqr_auth()
  q <- sprintf("SELECT * FROM [big-query-r:tests.realtime_markedmondsonme] ORDER BY ts DESC LIMIT %s", 
               limit)
  
  bqr_query(projectId = YOUR_PROJECT_ID, 
            datasetId = YOUR_DATASET_ID, 
            query = q, 
            useQueryCache = FALSE)  
}


get_bq <- function(){

  message("Getting new data...")
  check <- do_bq(1000)
  
  rt <- as.data.frame(check, stringsAsFactors = FALSE)
  names(rt) <- BIGQUERY_FIELDS
  
  ## turn string into JS timestamp
  rt$timestamp <- as.POSIXct(as.numeric(as.character(rt$ts)), origin="1970-01-01")
  
  rt
}

check_bq <- function(){
  
  check <- do_bq(1)
  
  message("Checking....check$ts: ", check$ts)
  check$ts
  
}

transform_rt <- function(rt){
  ## aggregate per hour
  rt_agg <- rt %>% 
    mutate(hour = format(timestamp, format = "%Y-%m-%d %H:00")) %>% 
    count(hour)
  
  rt_agg$hour <- as.POSIXct(rt_agg$hour, origin="1970-01-01")
  
  # ## the number of hits per timestamp
  rt_xts <- xts::xts(rt_agg$n, frequency = 24, order.by = rt_agg$hour)
  rt_ts <- ts(rt_agg$n, frequency = 24)
  
  list(forecast = forecast::forecast(rt_ts, h = 12),
       xts = rt_xts)
}

shinyServer(function(input, output, session) {
  
  ## checks every 5 seconds for changes
  realtime_data <- reactivePoll(5000, 
                                session, 
                                checkFunc = check_bq, 
                                valueFunc = get_bq)
  
  plot_data <- reactive({
    
    req(realtime_data())
    rt <- realtime_data()
    message("plot_data()")
    ## aggregate
    transform_rt(rt)
    
  })
  
  output$hc <- renderHighchart({
    
    req(plot_data())
    ## forcast values object
    fc <- plot_data()$forecast
    
    ## original data
    raw_data <- plot_data()$xts
    
    # plot last 48 hrs only, although forecast accounts for all data
    raw_data <- tail(raw_data, 48)
    raw_x_date <- as.numeric(index(raw_data)) * 1000
    
    ## start time in JS time
    forecast_x_start <- as.numeric(index(raw_data)[length(raw_data)])*1000
    ## each hour after that in seconds, 
    forecast_x_sequence <- seq(3600000, by = 3600000, length.out = 12)
    ## everything * 1000 to get to Javascript time
    forecast_times <- as.numeric(forecast_x_start + forecast_x_sequence)
    
    forecast_values <- as.numeric(fc$mean)
    
    hc <- highchart() %>%
      hc_chart(zoomType = "x") %>%
      hc_xAxis(type = "datetime") %>% 
      hc_add_series(type = "line",
                    name = "data",
                    data = list_parse2(data.frame(date = raw_x_date, 
                                                  value = raw_data))) %>%
      hc_add_series(type = "arearange", 
                    name = "80%",
                    fillOpacity = 0.3,
                    data = list_parse2(data.frame(date = forecast_times,
                                                  upper = as.numeric(fc$upper[,1]),
                                                  lower = as.numeric(fc$lower[,1])))) %>%
      hc_add_series(type = "arearange", 
                    name = "95%",
                    fillOpacity = 0.3,
                    data = list_parse2(data.frame(date = forecast_times,
                                                  upper = as.numeric(fc$upper[,2]),
                                                  lower = as.numeric(fc$lower[,2])))) %>% 
      hc_add_series(type = "line",
                    name = "forecast",
                    data = list_parse2(data.frame(date = forecast_times, 
                                                  value = forecast_values)))
    
    hc
    
  })
  

  

})
