library(shiny)
library(highcharter)

shinyUI(fluidPage(
  titlePanel("Realtime Shiny Dashboard from BigQuery"),
  highchartOutput("hc")
  )
)
