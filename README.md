# 📊 Smart Business Intelligence and Decision Support System (Smart BI)

A data-driven web application that transforms raw business data into meaningful insights and forecasts. This system enables organizations to make informed decisions through automated analysis, interactive dashboards, and predictive modeling.

---

## 🚀 Features

* 📁 Upload business datasets (CSV)
* 📊 Interactive dashboard with visual insights
* 📈 Sales trend analysis and performance metrics
* 🤖 Forecasting using machine learning models
* 🧠 Automated data processing and cleaning
* 💾 MySQL database integration for persistent storage
* 🌐 Fully deployed web application (Flask-based)

---

## 🛠️ Tech Stack

**Frontend**

* HTML
* CSS
* JavaScript
* Chart.js

**Backend**

* Python (Flask)

**Data Processing & ML**

* pandas
* NumPy
* scikit-learn

**Database**

* MySQL (Railway)

**Deployment**

* Render

---

## ⚙️ System Architecture

User uploads dataset → Backend processes data → Stores in MySQL → Performs analysis → Displays insights via dashboard

---

## 📂 Project Structure

```
Smart-BI/
│
├── app.py
├── config.py
├── requirements.txt
├── Procfile
├── .env
│
├── static/
│   ├── css/
│   ├── js/
│   └── uploads/
│
├── templates/
│   ├── index.html
│   └── dashboard.html
│
├── models/
├── utils/
└── data/
```

---

## 🧪 How to Run Locally

1. Clone the repository

```
git clone https://github.com/Chami-03/smart-bi-system.git
cd smart-bi
```

2. Install dependencies

```
pip install -r requirements.txt
```

3. Configure environment variables
   Create a `.env` file and add:

```
DB_HOST=your_host
DB_PORT=your_port
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=your_db
```

4. Run the application

```
python app.py
```

5. Open in browser

```
http://localhost:5000
```

---

## 🌍 Live Demo

👉 https://smart-bi-system.onrender.com

---

## 📌 Key Highlights

* Designed to simulate real-world business intelligence workflows
* Converts raw data into actionable insights
* Implements end-to-end pipeline: data ingestion → analysis → visualization → prediction
* Demonstrates practical use of data analytics and decision support systems

---

## 🎯 Future Improvements

* Role-based user authentication
* Advanced forecasting models
* Real-time data integration
* Export reports (PDF/Excel)
* Enhanced UI/UX for better user experience

---

## 👨‍💻 Author

Chamikara Wijerathne
Undergraduate | Information Systems Engineering
Interested in Business Analysis, Project Management, and Data Analytics

---

## ⭐ Show Your Support

If you like this project, feel free to give it a star ⭐ on GitHub!
