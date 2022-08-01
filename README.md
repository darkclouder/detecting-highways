# Detecting highways in road networks
Supplementary code base for evaluating road networks. I used this for my master thesis
[Detecting highways in road networks](https://asciico.de/assets/archive/papers/detecting-large-networks-thesis.pdf).

## Setup
You can either use an IDE to install the gradle dependencies or use `./gradlew` for Unix-based oses or `gradlew.bat` for Windows.

## Running

```
file FILE_PATH [WEIGHT (default: cost)] [DIRECTED (default: yes)] [ALGORITHM (default: edge_betweenness)] [META_FILE]
postgres NETWORK_ID POSTGRES_URL POSTGRES_USER [WEIGHT_COLUMN (default: cost)] [ALGORITHM (default: edge_betweenness)]
