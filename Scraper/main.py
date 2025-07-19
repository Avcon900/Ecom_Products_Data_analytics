from selenium import webdriver
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

import pandas as pd
import time
from datetime import datetime

categories = ['Headphones','Gaming Controllers','Refrigerator','Smart TV']  # Add more categories as needed

# Webdriver setup
driver = webdriver.Chrome()

def sendesc(browser):
    ActionChains(browser).send_keys(Keys.ESCAPE).perform()

all_dfs = []

for category in categories:
    # Lists to hold data for this category
    product_title = []
    Number_of_Reviews = []
    Rating = []
    Bought_last_month = []
    Price_after_Discount = []
    MRP = []
    Product_Image_urls = []
    Star_Rating_Percentage = []
    Category_col = []

    for page in range(1, 20):
        if page == 1:
            driver.get(f"https://www.amazon.in/s?k={category}")
        else:
            driver.get(f"https://www.amazon.in/s?k={category}&page={page}")
        wait = WebDriverWait(driver, 10)
        time.sleep(2)
        
        # Scroll to load lazy elements
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        for y in range(0, scroll_height, 300):
            driver.execute_script(f"window.scrollTo(0, {y});")
            time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        try:
            containers = wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-cy="asin-faceout-container"]'))
            )
        except:
            containers = []

        if not containers:
            continue
        
        for product in containers:
            # Title
            try:
                title = product.find_element(By.CSS_SELECTOR, '[data-cy="title-recipe"] h2 span').text
            except:
                title = "null"
            product_title.append(title)
            
            # Number of Reviews
            try:
                reviews = product.find_element(By.CSS_SELECTOR, '[data-cy="reviews-block"] a > span.a-size-base.s-underline-text').text
            except:
                reviews = "null"
            Number_of_Reviews.append(reviews)

            # Rating
            try:
                rating = product.find_element(By.CSS_SELECTOR, 'a.a-popover-trigger.a-declarative').get_attribute("aria-label")
            except:
                rating = "null"
            Rating.append(rating)

            # Bought last month
            try:
                bought = product.find_element(By.CSS_SELECTOR, '[data-cy="reviews-block"] span.a-size-base.a-color-secondary').text
            except:
                bought = "null"
            Bought_last_month.append(bought)

            # Price after Discount
            try:
                price = product.find_element(By.CLASS_NAME, 'a-price-whole').text
            except:
                price = "null"
            Price_after_Discount.append(price)

            # MRP
            try:
                mrp = product.find_element(By.CLASS_NAME, 'a-text-price').text
            except:
                mrp = "null"
            MRP.append(mrp)

            # Product Image URL
            time.sleep(1)
            try:
                wait.until(
                    EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'div.a-section.aok-relative.s-image-fixed-height img'))
                )
                img_url = product.find_element(By.CSS_SELECTOR, 'div.a-section.aok-relative.s-image-fixed-height img').get_attribute('src')
            except:
                img_url = "null"
            Product_Image_urls.append(img_url)

            # Percentage of Individual Star Rating
            rating_dist = {i: "null" for i in range(5, 0, -1)}
            try:
                icon = product.find_element(By.CSS_SELECTOR, 'i.a-icon.a-icon-popover')
                driver.execute_script("arguments[0].scrollIntoView(true);", icon)
                time.sleep(1)
                icon.click()
                time.sleep(3)

                popover_divs = driver.find_elements(By.CSS_SELECTOR, "div.a-popover")
                visible_popover = None
                for div in popover_divs:
                    style = div.get_attribute("style") or ""
                    if "display: none" not in style:
                        try:
                            histo_table = div.find_element(By.ID, "histogramTable")
                            visible_popover = div
                            break
                        except:
                            continue
                time.sleep(1.5)
                if visible_popover:
                    rows = histo_table.find_elements(By.CSS_SELECTOR, "li")
                    if rows and len(rows) == 5:
                        for row in rows:
                            try:
                                star_text = row.find_element(By.CSS_SELECTOR, ".a-text-left").text.strip()
                                percent_text = row.find_element(By.CSS_SELECTOR, ".a-text-right").text.strip()
                                star_value = int(star_text[0])
                                rating_dist[star_value] = percent_text
                            except:
                                continue
                time.sleep(1)
                sendesc(driver)
            except:
                pass

            Star_Rating_Percentage.append(rating_dist)
            Category_col.append(category)

    # DataFrame for this category
    df = pd.DataFrame({
        "Title": product_title,
        "Number_of_Reviews": Number_of_Reviews,
        "Rating": Rating,
        "Bought_Last_Month": Bought_last_month,
        "Price_After_Discount": Price_after_Discount,
        "MRP": MRP,
        "Image_URL": Product_Image_urls,
        "Star_Rating_Percentage": Star_Rating_Percentage,
        "Category": Category_col
    })
    df['Product_ID'] = range(1, len(df) + 1)
    all_dfs.append(df)

# Close the driver after scraping
driver.quit()

# Concatenate all DataFrames
final_df = pd.concat(all_dfs, ignore_index=True)

# Capture end‑of‑run timestamp
run_time = datetime.now()
safe_ts = run_time.strftime('%Y%m%d_%H%M%S')    # e.g. "20250718_154230"

# Add TIMESTAMP column if desired (same value for all rows)
final_df['TIMESTAMP'] = run_time.strftime('%Y-%m-%d %H:%M:%S')

# Build filename and save
filename = f"data/{safe_ts}.csv"
final_df.to_csv(filename, index=False)
print(f"Saved output to {filename}")
