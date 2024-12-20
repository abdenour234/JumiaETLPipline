import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Setting up headless Chrome
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run headless
chrome_options.add_argument("--disable-gpu")  # Disable GPU hardware acceleration
chrome_options.add_argument("--no-sandbox")  # Bypass OS security model (Linux)
chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

def get_links(i):
    base = f"https://www.jumia.ma/ordinateurs-accessoires-informatique/all-products/?page={i}#catalog-listing"
    driver.get(base)
    driver.implicitly_wait(10)
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    Products = soup.find_all('a', class_='core', href=True)
    Links = ["https://www.jumia.ma/" + k['href'] for k in Products]
    return Links

def get_all_links(limit):
    Links = []
    i = 1
    while i <= limit:  # Adjust the range as per requirement
        Links.extend(get_links(i))
        i = i + 1
    return Links

def scrap(link):
    driver.get(link)
    driver.implicitly_wait(10)
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    try:
        name = soup.find('div', class_="-pls -prl").text
    except:
        name = "Nan"
    
    try:
        marque = soup.find_all('div',class_="-pvxs")[1].text.split('|')[0]
        #marque=marque.find('div',class_="-pvxs")
        #print(marque)
    except:
        marque = "Nan"
        #print(marque)
    
    try:
        prix = soup.find('span', class_="-b -ubpt -tal -fs24 -prxs").text
    except:
        prix = "Nan"
    
    try:
        anc_prix = soup.find('span', class_="-tal -gy5 -lthr -fs16 -pvxs -ubpt").text
    except:
        anc_prix = prix
    
    try:
        discount = soup.find('span', class_="bdg _dsct _dyn -mls").text
    except:
        discount = 0
    
    try:
        stock = soup.find('p', class_="-df -i-ctr -fs12 -pbs -gy5").text
    except:
        try:
            stock = soup.find('p', class_="-df -i-ctr -fs12 -pbs -rd5").text 
        except:
            stock = "Nan"
    
    try:
        rats = soup.find('div', class_="-df -i-ctr -pbs")
        rating = rats.find('div', class_="stars _m _al").text
        reviews_num = rats.find('a', class_="-plxs _more").text
    except:
        rating, reviews_num = 'Nan', 'Nan'
    
    try:
        coments = soup.find_all('article', class_="-pvs -hr _bet")
        Comentaire1 = coments[0].text
        Comentaire2 = coments[1].text
    except:
        Comentaire1, Comentaire2 = "Nan", "Nan"
    
    try:
        infos_sur_vendeur = soup.find('div', class_="-df -j-bet -fs12").text
    except:
        infos_sur_vendeur = "Nan"
    
    try:
        Performance_vendeur = soup.find('div', class_="-pas -bt -fs12").text
    except:
        Performance_vendeur = "Nan"
    
    # Return the scraped data as a dictionary
    return {
        'Name': name,
        'Brand': marque,
        'Price': prix,
        'Old Price': anc_prix,
        'Discount': discount,
        'Stock': stock,
        'Rating': rating,
        'Reviews': reviews_num,
        'First Comment': Comentaire1,
        'Second Comment': Comentaire2,
        'Seller Info': infos_sur_vendeur,
        'Seller Performance': Performance_vendeur
    }

def extract(limit=1):
    links = get_all_links(limit)  # Get all links
    data = []  # Initialize list to hold all data
    
    for link in links:
        product_data = scrap(link)  # Scrape each link
        data.append(product_data)  # Append scraped data to list
    
    # Create DataFrame from the list of dictionaries
    df = pd.DataFrame(data)
    
    # Save DataFrame to CSV file
    #df.to_csv("jumia_products.csv", index=False)
    
    return df
