import matplotlib.pyplot as plt


from utils.paths import RAW_DATA_PATH, CLEAN_DATA_PATH
from utils.stats import count_corpus_per_month, count_corpus_per_day, count_languages, count_hashtag_per_day


def plot_month_counts(data_path: str):
    month_dict = count_corpus_per_month(data_path)
    lists = month_dict.items()
    x, y = zip(*lists)
    plt.plot(x, y)
    plt.xticks(rotation=45)
    plt.show()
    
    
def plot_days_counts(data_path: str):
    month_dict = count_corpus_per_day(data_path)
    lists = month_dict.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    plt.plot(x, y)
    plt.xticks(rotation=45)
    every_nth = 15
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)
    plt.show()


def plot_hashtag_per_day_count(data_path: str, hashtag: str):
    counter = count_hashtag_per_day(data_path, hashtag)
    lists = counter.items()
    x, y = zip(*lists)
    fig, ax = plt.subplots()
    plt.plot(x, y)
    plt.xticks(rotation=45)
    every_nth = 15
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)
    plt.show()
    
    
def plot_languages(data_path: str):
    lang_dict = count_languages(data_path)
    lang_dict.pop("ja")
    plt.bar(list(lang_dict.keys()), lang_dict.values())
    plt.show()


if __name__ == "__main__":
    plot_hashtag_per_day_count(CLEAN_DATA_PATH, "#timesup")
