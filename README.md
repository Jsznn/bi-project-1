# Global Digital Skills Analytics 🌍📊

**Business Intelligence & Data Warehouse Final Project**

A comprehensive Business Intelligence solution designed to analyze, visualize, and interpret global digital skills data. This project aggregates data from international sources to provide insights into the digital maturity of nations and economic zones.

## 👥 Contributors

*   **Jason Jesse Joel Polii**
*   **Weslie Austin**
*   **Ferdiantono**

---

## 🚀 Project Overview

This project implements a full ETL (Extract, Transform, Load) pipeline and a modern web dashboard to track the evolution of digital skills worldwide. By focusing on "Basic" versus "Above Basic" (Advanced) digital skills, we aim to understand not just who is online, but who is creating value in the digital economy.

### 🎯 Key Performance Indicators (KPIs)

We track four core metrics to evaluate digital maturity:

1.  **Digital Literacy Rate** 📖
    *   *Definition:* The percentage of the population possessing "Basic" vs. "Above Basic" computer skills.
    *   *Purpose:* Serves as the baseline metric for national digital competency.

2.  **Skill Depth Ratio** 🧠
    *   *Formula:* $\frac{\text{Above Basic \%}}{\text{Basic \%}}$
    *   *Interpretation:* A higher ratio indicates a workforce that isn't just literate but highly skilled. It measures the conversion of basic users into advanced creators.

3.  **Year-over-Year (YoY) Growth** 📈
    *   *Formula:* $\frac{(\text{Current Year \%} - \text{Previous Year \%})}{\text{Previous Year \%}}$
    *   *Purpose:* Measures the velocity of skill acquisition. How fast is a population upskilling?

4.  **Regional Maturity Index** 🌐
    *   *Definition:* Aggregated scores for broader economic zones (e.g., Euro Area, OECD, Arab World).
    *   *Purpose:* Allows for macro-economic comparisons beyond individual country performance.

---

## 💡 Key Insights & Questions

The dashboard is designed to answer critical business questions:

*   **🏆 Who are the Advanced Creators?**
    *   Which countries have the highest density of "Above Basic" skills? We compare leaders like South Korea against the global average to identify hubs of digital innovation.
*   **⚖️ Is the Digital Divide Widening?**
    *   By comparing the YoY growth rates of top-tier nations vs. bottom-tier nations, we determine if lagging countries are catching up or falling further behind.
*   **🔗 The Skill Correlation**
    *   What is the relationship between "Basic" and "Above Basic" skills? Do countries with high basic literacy automatically develop advanced skills, or is there a "middle-skill trap" (drop-off)?

---

## 🛠️ Technology Stack

*   **ETL Pipeline:** Python, Pandas, SQLAlchemy
*   **Database:** PostgreSQL (Supabase)
*   **Backend API:** FastAPI (Python)
*   **Frontend:** HTML5, Tailwind CSS, Chart.js
*   **Deployment:** Vercel (Serverless)

---

## ⚙️ Setup & Installation

### Prerequisites
*   Python 3.9+
*   PostgreSQL Database (or Supabase URL)

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/bi-project.git
cd bi-project
```

### 2. Environment Setup
Create a `.env` file in the root directory:
```env
DATABASE_URL=postgresql://user:password@host:port/dbname
```

### 3. Install Dependencies
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Mac/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 4. Run the ETL Pipeline
Extract data from CSV, transform it, and load it into the database:
```bash
python etl.py
```

### 5. Run the Web Application
Start the FastAPI backend locally:
```bash
uvicorn api.index:app --reload
```
Visit `http://127.0.0.1:8000/public/index.html` (or serve the static file separately) to view the dashboard.

---

## 📂 Project Structure

```
bi-project/
├── api/                  # Backend API (FastAPI)
│   └── index.py
├── data/                 # Raw Data Sources (CSV)
├── public/               # Frontend Dashboard (HTML/JS)
│   └── index.html
├── etl.py                # ETL Pipeline Script
├── create_ict_skills.sql # SQL Schema
├── requirements.txt      # Python Dependencies
└── vercel.json           # Deployment Config
```
