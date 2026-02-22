## ☁️ Deployment (Render)
This application is configured to be easily deployed on Render. 

**Important Note on Storage:** Because Render uses an ephemeral filesystem, any extracted CSV datasets will be permanently deleted when the server goes to sleep or restarts. Always download your extracted files immediately after the process finishes!

1. Connect your GitHub repository to a new Web Service in Render.
2. Build Command: `pip install -r requirements.txt`
3. Start Command: `gunicorn run:app`

## 🔍 Source Transparency
This tool acts solely as an aggregator and formatting pipeline. All data is retrieved directly from the public web services of the Armenian government.

* **Data Source:** https://cpcarmenia.am/
* **API Documentation:** https://cpcarmenia.am/wp-content/uploads/2024/08/API_Manual_v1.2-ARM-1.pdf

## 👨‍💻 Author
Built by **Aren Nazaryan** for data journalists and researchers analyzing public accountability in Armenia.

## 📄 License
This project is licensed under the [PolyForm Noncommercial License 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0/) provided by the PolyForm Project. See the `LICENSE` file for details.

You are free to use, modify, and distribute this software for personal, academic, journalistic, or non-profit purposes. Commercial use, including integrating this tool into a paid service, selling the output data, or using it to generate revenue, is strictly prohibited.
