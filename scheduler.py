import time
import schedule
from jobs import etf_stats_job, stocks_stats_job


def main():
    schedule.every().monday.at("09:00").do(etf_stats_job)
    schedule.every(3).days.at("09:30").do(stocks_stats_job)

    while True:
        schedule.run_pending()
        time.sleep(1)
        stocks_stats_job()  # DEL
        # etf_stats_job()  # DEL
        break


if __name__ == "__main__":
    main()
