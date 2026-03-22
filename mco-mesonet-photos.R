library(tidyverse)
library(sf)
library(magrittr)

# Custom Albers parameterization from Alex Stum, 2021-10-20
umrb_grid_proj <-
  list(proj = "aea",
       lat_0 = 41.8865,
       lat_1 = 43.0,
       lat_2 = 47.8,
       lon_0 = -104.0487,
       units = "mi") %>%
  {paste0("+",names(.),"=",., collapse = " ")} %>%
  sf::st_crs()

edge <- 
  sqrt(500)

mt_grid <-
  raster::raster(crs = umrb_grid_proj$input,
                 resolution = c(edge,edge),
                 xmn = 0-(25*edge),
                 ymn = 0,
                 xmx = (19*edge),
                 ymx = 0+(24*edge)
  ) %>%
  raster::rasterToPolygons() %>%
  sf::st_as_sf() %>%
  sf::st_filter(
    mcor::mt_state_simple |>
      sf::st_transform(umrb_grid_proj)
  ) |>
  sf::st_join(sf::read_sf("data/fwmesonetgrid19oct2021") %>%
                sf::st_transform(umrb_grid_proj) %>%
                dplyr::filter(Status != "Less than 40%") %>%
                sf::st_centroid(),
              join = sf::st_contains) %>%
  dplyr::filter(!is.na(cell)) %>%
  dplyr::select(`Grid Cell ID` = cell)

mesonet_stations <- 
  readr::read_csv("https://mesonet.climate.umt.edu/api/stations?type=csv") %>%
  dplyr::filter(sub_network == "HydroMet") %>%
  dplyr::select(station, name, date_installed, nwsli_id)

umrb_status <-
  readxl::read_excel("data/Site Status Databas 10 MARCH 2026.xlsx") %>%
  dplyr::filter(`Station Status` != "Removed",
                State == "Montana",
                !is.na(`NWS LI ID`)) %>%
  dplyr::select(`Grid Cell ID`,
                nwsli_id = `NWS LI ID`,
                `Station Status`) %>%
  tidyr::separate_wider_delim(`Grid Cell ID`, 
                              delim = stringr::regex("-| "),
                              names = c("Cell","Number"),
                              too_few = "align_start") %>%
  dplyr::mutate(Number = as.numeric(Number)) %>%
  tidyr::unite(
    col = `Grid Cell ID`, 
    c(Cell, Number),
    sep = "-"
  )

mesonet_stations_sf <-
  mesonet_stations %>%
  dplyr::left_join(umrb_status, 
                   by = "nwsli_id") %>%
  dplyr::arrange(nwsli_id) %>%
  dplyr::filter(!is.na(`Grid Cell ID`)) %>%
  dplyr::left_join(mt_grid) %>%
  sf::st_as_sf() %>%
  sf::st_transform(umrb_grid_proj) %>%
  dplyr::select(station, name) %>%
  sf::st_transform("EPSG:4326") %T>%
  sf::write_sf("docs/stations.geojson",
               delete_dsn = TRUE)

