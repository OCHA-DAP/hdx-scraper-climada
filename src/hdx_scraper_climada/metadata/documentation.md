##climada-litpop-dataset.notes##
A high-resolution asset exposure dataset produced using 'lit population' (LitPop), a globally consistent methodology to disaggregate asset value data proportional to a combination of nightlight intensity and geographical population data. Exposure data for population, asset values and productive capital at 4km spatial resolution globally, consistent across country borders. The dataset offers value for manifold use cases, including globally consistent economic disaster risk assessments and climate change adaptation studies, especially for larger regions, yet at considerably high resolution. The [Climada Data API](https://climada.ethz.ch/data-types/) can be used to explore the full, original datasets.

##climada-litpop-dataset.methodology_other##
Created from the Climada API. Background described in Eberenz, S., Stocker, D., Röösli, T., and Bresch, D. N., 2020: Asset exposure data for global physical risk assessment, Earth Syst. Sci. Data, 12, 817-833, https://doi.org/10.5194/essd-12-817-2020. Dataset for 224 countries also archived in the ETH Research Repository with the link https://doi.org/10.3929/ethz-b-000331316 (Eberenz et al., 2019).

##climada-litpop-dataset.caveats##
2024-01-10: There is an issue with the Syria data so it is not included, this issue is being addressed
In this API we provide datasets in a form that can readily be used in CLIMADA analyses.

Users should determine whether these datasets are suitable for a particular purpose or application, considering factors such as resolution (for example, a 4km grid is not suitable for modelling risk at the neighborhood level), the way that hazards are represented in the dataset (for example, specific events, event thresholds, probabilistic event sets, etc.), the way that exposure is represented, and other aspects.

Data provided with no warranty of any kind under CC BY 4.0.

See respective API metadata and referenced publications for details and limitations.

##climada-crop-production-dataset.notes##
Historical and twenty-first century crop production in tons. Global gridded (4km resolution) crop yield simulations for maize, rice, soybean, and wheat, encompassing an ensemble of transient yield simulation output from eight global gridded crop models driven by bias-corrected output from five global climate models, as facilitated by the Inter-Sectoral Impact Model Intercomparison Project (ISIMIP, isimip.org)

##climada-crop-production-dataset.methodology_other##
Created from the Climada API. Background described in Eberenz, S., 2021: Globally Consistent Assessment of Climate-related Physical Risk. A Conceptual Framework and its Application in Asset Valuation. Doctoral Thesis, ETH Zurich, Switzerland. https://doi.org/10.3929/ethz-b-000489485

##climada-crop-production-dataset.caveats##
In this API we provide datasets in a form that can readily be used in CLIMADA analyses.

Users should determine whether these datasets are suitable for a particular purpose or application, considering factors such as resolution (for example, a 4km grid is not suitable for modelling risk at the neighborhood level), the way that hazards are represented in the dataset (for example, specific events, event thresholds, probabilistic event sets, etc.), the way that exposure is represented, and other aspects.

Data provided with no warranty of any kind under CC BY 4.0.

See respective API metadata and referenced publications for details and limitations.

##climada-earthquake-dataset.notes##
Earthquake hazard sets at 150 arcsec (ca. 4km) resolution, available for the entire globe and per country. Available as historic records from the USGS epicentres database and as a simple probabilistic sampling starting from the historic earthquake catalog, with 9 synthetic events per historic record. The presented data are maximum intensity on the Modified Mercalli intensity scale (MMI) over the historic record 1905-2017.

##climada-earthquake-dataset.methodology_other##
Created from the Climada API: https://github.com/CLIMADA-project/climada_petals/tree/feature/quake. Please note that there is currently no peer-reviewed publication documenting this hazard set.

##climada-earthquake-dataset.caveats##
In this API we provide datasets in a form that can readily be used in CLIMADA analyses.

Users should determine whether these datasets are suitable for a particular purpose or application, considering factors such as resolution (for example, a 4km grid is not suitable for modelling risk at the neighborhood level), the way that hazards are represented in the dataset (for example, specific events, event thresholds, probabilistic event sets, etc.), the way that exposure is represented, and other aspects.

Data provided with no warranty of any kind under CC BY 4.0.

See respective API metadata and referenced publications for details and limitations.

##climada-flood-dataset.notes##
Flood footprint of historical events at a 200m x 200m resolution based on the cloud to street database with events ranging from the years 2002-2018 (see https://floodbase.com). The events have been processed into one hazard dataset per country.

##climada-flood-dataset.methodology_other##
Created from the Climada API

##climada-flood-dataset.caveats##
2024-02-14: No data from CLIMADA for Burkina Faso, Cameroon, South Sudan and the State of Palestine.
In this API we provide datasets in a form that can readily be used in CLIMADA analyses.

Users should determine whether these datasets are suitable for a particular purpose or application, considering factors such as resolution (for example, a 4km grid is not suitable for modelling risk at the neighborhood level), the way that hazards are represented in the dataset (for example, specific events, event thresholds, probabilistic event sets, etc.), the way that exposure is represented, and other aspects.

Data provided with no warranty of any kind under CC BY 4.0.

See respective API metadata and referenced publications for details and limitations.

##climada-wildfire-dataset.notes##
Global wildfire dataset at 4km resolution, based on MODIS satellite data 2000-2021 (cf https://firms.modaps.eosdis.nasa.gov).

##climada-wildfire-dataset.methodology_other##
Created from the Climada API: Background described in Lüthi, Aznar-Siguan, G., Fairless, C., and Bresch, D. N., 2021: Globally consistent assessment of economic impacts of wildfires in CLIMADA v2.2, Geosci. Model Dev., 14, 7175-7187, https://gmd.copernicus.org/articles/14/7175/2021/

##climada-wildfire-dataset.caveats##
In this API we provide datasets in a form that can readily be used in CLIMADA analyses.

Users should determine whether these datasets are suitable for a particular purpose or application, considering factors such as resolution (for example, a 4km grid is not suitable for modelling risk at the neighborhood level), the way that hazards are represented in the dataset (for example, specific events, event thresholds, probabilistic event sets, etc.), the way that exposure is represented, and other aspects.

Data provided with no warranty of any kind under CC BY 4.0.

See respective API metadata and referenced publications for details and limitations.

##climada-river-flood-dataset.notes##
River flood [flood depth in meters and flooded area fraction] footprints worldwide at 150 arcsec (approx 4 kilometers at equator) resolution. Based on isimip.org CaMa flood output derived from a series of global circulation model output, combined into a composite hazard event set. Data available for each (flood exposed) country worldwide. Versions exist for flood hazard today today plus select IPCC representative concentration pathways (rcp) emission scenarios for select future time periods.

##climada-river-flood-dataset.methodology_other##
Created from the Climada API. Background described in Sauer, I., Reese, R., Otto, C., Geiger, T., Willner, S. N., Guillod, B., David N. Bresch and Frieler, K., 2021: Climate signals in river flood damages emerge under sound regional disaggregation. Nature Communications, 12(1), 1-11. https://www.nature.com/articles/s41467-021-22153-9. and Kam, P. M., Aznar-Siguan, G., Schewe, J., Milano, L., Ginnetti, J., Willner, S., McCaughey, J., and Bresch, D., N., 2021: Global warming and population change both heighten future risk of human displacement due to river floods. Environ. Res. Lett. 16, 044026. https://doi.org/10.1088/1748-9326/abd26c


##climada-river-flood-dataset.caveats##
In this API we provide datasets in a form that can readily be used in CLIMADA analyses.

Users should determine whether these datasets are suitable for a particular purpose or application, considering factors such as resolution (for example, a 4km grid is not suitable for modelling risk at the neighborhood level), the way that hazards are represented in the dataset (for example, specific events, event thresholds, probabilistic event sets, etc.), the way that exposure is represented, and other aspects.

Data provided with no warranty of any kind under CC BY 4.0.

See respective API metadata and referenced publications for details and limitations.

##climada-tropical-cyclone-dataset.notes##
Tropical cyclone wind footprints (m/s) at 150 arcsec (approx 4 kilometers at equator) resolution. Available as global files and per country; available for historically observed records, and synthetically created, probabilistic events, from various modelling sources, for present and future climate scenarios.

##climada-tropical-cyclone-dataset.methodology_other##
Created from the Climada API. Background described in Eberenz, S., Lüthi, S., and Bresch, D. N., 2021: Regional tropical cyclone impact functions for globally consistent risk assessments, Nat. Hazards Earth Syst. Sci., 21, 393-415, https://doi.org/10.5194/nhess-21-393-2021.
Aznar-Siguan, G., and Bresch, D. N., 2019: CLIMADA v1: a global weather and climate risk assessment platform, Geosci. Model Dev., 12, 3085–3097. https://doi.org/10.5194/gmd-12-3085-2019.
Bresch, D. N. and Aznar-Siguan, G., 2021: CLIMADA v1.4.1: towards a globally consistent adaptation options appraisal tool, Geosci. Model Dev., 14, 351-363, https://doi.org/10.5194/gmd-14-351-2021

##climada-tropical-cyclone-production-dataset.caveats##
In this API we provide datasets in a form that can readily be used in CLIMADA analyses.

Users should determine whether these datasets are suitable for a particular purpose or application, considering factors such as resolution (for example, a 4km grid is not suitable for modelling risk at the neighborhood level), the way that hazards are represented in the dataset (for example, specific events, event thresholds, probabilistic event sets, etc.), the way that exposure is represented, and other aspects.

Data provided with no warranty of any kind under CC BY 4.0.

See respective API metadata and referenced publications for details and limitations.

##climada-storm-europe-dataset.notes##
European winter storm [gust in meters per second] footprints for Europe. Data is available from two projects: 1. Based on Coperncus WISC (https://wisc.climate.copernicus.eu/wisc) historical event footprints 1940-2014 plus 50 probabilistic events per original event, at 4 kilometers resolution. Data available for European countries, and corresponds to the storm hazard today. See key reference Röösli et al. 2021. 2. Winter storm hazards generated from ERA5 (1980-2010) and CMIP6 models (1980-2010 and 2070-2100). In that second case, only Europe wide datasets are available, with lower resolution (depending on the GCM or ERA5 model). See Severino et al. 2023.

##climada-storm-europe-dataset.methodology_other##
Created from the Climada API. Background described in Welker, C., Röösli, T., and Bresch, D. N., 2021: Comparing an insurer’s perspective on building damages with modelled damages from pan-European winter windstorm event sets: a case study from Zurich, Switzerland. Nat. Hazards Earth Syst. Sci., 21, 279-299. https://www.nat-hazards-earth-syst-sci-discuss.net/nhess-2020-115.
Röösli, T., Appenzeller, C., and Bresch, D. N., 2021: Towards operational impact forecasting of building damage from winter windstorms in Switzerland. Meteorol Appl. 28:e2035. http://dx.doi.org/10.1002/met.2035
Severino, L. G., Kropf, C. M., Afargan-Gerstman, H., Fairless, C., de Vries, A. J., Domeisen, D. I. V., and Bresch, D. N.: Projections and uncertainties of future winter windstorm damage in Europe, EGUsphere [preprint], https://doi.org/10.5194/egusphere-2023-205, 2023.

##climada-storm-europe-dataset.caveats##
2024-01-15: Only Ukraine data since other HRP countries are not in the source
In this API we provide datasets in a form that can readily be used in CLIMADA analyses.

Users should determine whether these datasets are suitable for a particular purpose or application, considering factors such as resolution (for example, a 4km grid is not suitable for modelling risk at the neighborhood level), the way that hazards are represented in the dataset (for example, specific events, event thresholds, probabilistic event sets, etc.), the way that exposure is represented, and other aspects.

Data provided with no warranty of any kind under CC BY 4.0.

See respective API metadata and referenced publications for details and limitations.
