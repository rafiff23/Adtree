# Adtree — Creator & Agency Management Platform

Adtree is a comprehensive platform for managing creator partnerships, tracking performance metrics, and facilitating payments between agencies and creators. Built with Streamlit for the dashboard and Flask for APIs, Adtree connects TikTok creators with agencies and provides real-time leaderboards, performance tracking, and administrative tools.

---

## 🎯 Core Features

### **1. Leaderboard System**
Dynamic, event-based leaderboards that track creator performance across multiple metrics:
- **Monthly & Weekly Rankings**: Track creators by GMV (revenue), post count, viral potential, and total views
- **Industry Filtering**: Accommodations, Dining, Things to Do
- **Creator Levels**: Tier creators by performance (Lv.1 → Lv.4)
- **Multi-Event Support**: Run multiple competitions simultaneously (Birthday Challenge, Ramadan Campaign, etc.)
- **Real-time Updates**: Import data weekly via Excel, automatically recalculate rankings

**Documentation:** [leaderboard/README.md](leaderboard/README.md)

### **2. Agency Target Management**
Set and track performance targets for each agency by industry and week:
- Create, edit, and delete targets
- Organize by agency and industry
- View weekly breakdowns and totals
- Filter by agency with performance metrics

**Access:** Main app sidebar → "Agency Target"

### **3. Creator Registry & API**
REST API for external clients to query creator data:
- Get all creators (paginated)
- Filter by agency or binding status
- Bulk export as JSON or CSV
- Authentication via X-API-Key header
- Running on port 8006

**Documentation:** [API_CREATOR_README.md](API_CREATOR_README.md)  
**Base URL:** `https://api.adtreedigital.cloud`

### **4. Creator Onboarding**
Manage the creator registration and binding process:
- Bulk import creator data from Excel
- Track binding status (Bound/Unbound)
- Assign creators to agencies
- Store contact information and TikTok metrics

### **5. Content Submission & Invoicing**
Administrative tools for:
- Content submission tracking
- Invoice generation and management
- Payment processing
- Business Development operations (bdops)

---

## 🏗️ System Architecture

### **Tech Stack**
- **Frontend**: Streamlit (Python web dashboard)
- **Backend**: Python (database queries, calculations)
- **API**: Flask (RESTful endpoints)
- **Database**: PostgreSQL (hosted on same VPS)
- **Hosting**: Self-hosted VPS (72.61.143.167, Ubuntu 22.04)
- **Reverse Proxy**: Nginx with SSL/TLS (Let's Encrypt)

### **Project Structure**
```
/Adtree
├── README.md                    # This file
├── CLAUDE.md                    # n8n workflow builder config
├── creator.py                   # Main Streamlit app entry point
├── db.py                        # Database functions
├── api_creator.py               # Creator API (Flask)
├── agency_target_page.py        # Agency target management page
├── leaderboard/                 # Leaderboard system
│   ├── README.md               # Detailed leaderboard documentation
│   ├── event_page.py           # Event management UI
│   ├── leaderboard_page.py     # Leaderboard display
│   └── ...
├── creator/                     # Creator management pages
├── invoice/                     # Invoicing module
├── bdops/                       # Business Development tools
├── onboarding.py               # Creator onboarding
├── content_submission.py        # Content tracking
├── settings.py                 # Admin settings
└── requirements.txt            # Python dependencies
```

---

## 📊 Key Workflows

### **Leaderboard Ranking Flow**
1. Admin uploads event data via Settings page (weekly/monthly)
2. System imports Excel file → PostgreSQL
3. Rankings automatically calculated by metric (GMV, posts, views, virality)
4. Creators filtered by industry and level tier
5. Leaderboard page displays real-time rankings

### **Agency Target Setting**
1. Admin navigates to "Agency Target" page
2. Selects agency + industry, sets total and weekly targets
3. Data saved to database
4. Can edit/delete existing targets anytime
5. Tracks target vs. actual performance

### **Creator Data Access (API)**
1. External client gets API key from admin
2. Makes authenticated request to `/api/creators`
3. Can filter by agency, binding status, pagination
4. Bulk export available as JSON or CSV
5. All responses include agency name and creator details

---

## 🚀 Deployment

### **Streamlit App** (Main Dashboard)
```bash
cd /Users/rafif/Documents/GitHub/Adtree
source .venv/bin/activate
streamlit run creator.py
```
Accessible at: `https://app.adtreedigital.cloud` (via Nginx proxy)

### **Creator API** (Flask)
```bash
cd ~/apps/creator_api
API_KEY='your-secret-key' nohup ./venv/bin/python api_creator.py > creator_api.log 2>&1 &
```
Running on port 8006, proxied to: `https://api.adtreedigital.cloud`

### **VPS Management**
```bash
# SSH to VPS
ssh adtree@72.61.143.167 -i ~/.ssh/id_ed25519

# View n8n container logs
docker -C ~/apps/n8n logs n8n --tail 50
```

---

## 🔐 Authentication & Security

- **Streamlit App**: Session-based authentication (check CLAUDE.md or settings.py)
- **Creator API**: X-API-Key header (generated per client)
- **Database**: PostgreSQL user: `postgres`, accessible from VPS only
- **SSL/TLS**: Let's Encrypt certificates via Certbot, auto-renewed

**Security Notes:**
- Never commit real API keys or database passwords
- Use environment variables for all secrets
- API keys are unique per client
- All external APIs require authentication headers

---

## 📈 Database Schema Overview

### **Main Tables**
- `public.agency_map`: Agency information and IDs
- `public.creator_registry`: All creator data (TikTok ID, followers, binding status, level, etc.)
- `leaderboard.events`: Event/campaign configuration (monthly/weekly, metrics, industries)
- `leaderboard.tiktok_go_video_summary`: Raw transaction and performance data
- `leaderboard.creator_performance`: Weekly aggregated rankings by metric
- `target.agency_target`: Agency performance targets by industry and week

See [leaderboard/README.md](leaderboard/README.md) for detailed schema documentation.

---

## 🛠️ Development & Customization

### **Adding a New Event**
1. Go to Settings → Create Event
2. Define: name, month/week, metrics (GMV, posts, views, virality)
3. Select industries (Accommodations, Dining, Things to Do)
4. Set creator level eligibility
5. Upload data via Excel import

### **Extending the API**
Edit `api_creator.py` to add new endpoints. All endpoints must:
- Require `@require_api_key` decorator
- Use parameterized queries (prevent SQL injection)
- Return consistent JSON format: `{"success": true, "data": [...], "error": null}`

### **Adding a New Page to the Dashboard**
1. Create `new_page.py` in root directory
2. Define `render()` function that calls Streamlit commands
3. Import and add to sidebar in `creator.py`
4. Deploy with `streamlit run creator.py`

---

## 📚 Detailed Documentation

- **[Leaderboard System](leaderboard/README.md)**: How rankings work, metrics, tiers, adding events
- **[Creator API](API_CREATOR_README.md)**: Endpoints, authentication, client setup, examples
- **[n8n Workflows](CLAUDE.md)**: Automation setup with Claude AI
- **[bdops/README.md](bdops/README.md)**: Business development operations

---

## ❓ FAQ & Troubleshooting

**Q: How do I reset creator rankings?**  
A: Delete the event and all associated data will be cleared. Recreate the event with new data.

**Q: The API is returning 401 Unauthorized**  
A: Check that the X-API-Key header is correct and matches the key generated in settings.

**Q: How often are rankings updated?**  
A: Rankings update whenever new data is imported via the Settings page. Typically weekly.

**Q: Can creators see their own rankings?**  
A: Yes, the Leaderboard page is public. No authentication required to view.

**Q: How is currency handled?**  
A: Fixed conversion rate: 1 USD = 16,000 IDR for all GMV calculations.

---

## 📞 Support & Maintenance

For issues or feature requests:
1. Check the relevant documentation file
2. Review database logs: `SELECT * FROM leaderboard.events` for event issues
3. Check Streamlit app logs for UI errors
4. Check Flask API logs: `tail -f ~/apps/creator_api/creator_api.log`
5. Verify VPS connectivity: `ssh adtree@72.61.143.167 -i ~/.ssh/id_ed25519`

---

**Last Updated**: April 2026  
**Version**: 1.0  
**Platform**: Streamlit + Flask + PostgreSQL on Ubuntu 22.04
