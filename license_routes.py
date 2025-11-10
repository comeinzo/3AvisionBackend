from flask import Blueprint, request, jsonify
from psycopg2 import connect, extras
from datetime import datetime

license_bp = Blueprint('license_bp', __name__)

# Database connection helper
def get_connection():
    return connect(
        host="localhost",
        database="your_database",
        user="postgres",
        password="your_password"
    )

# 1️⃣ Add new license plan
@license_bp.route('/add_license_plan', methods=['POST'])
def add_license_plan():
    data = request.json
    plan_name = data.get('plan_name')
    description = data.get('description', '')
    storage_limit = data.get('storage_limit', 0)
    price = data.get('price', 0)

    print("📦 Adding License Plan:", plan_name)

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO license_plan (plan_name, description, storage_limit, price)
            VALUES (%s, %s, %s, %s)
        """, (plan_name, description, storage_limit, price))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'message': 'License plan added successfully'}), 200
    except Exception as e:
        print("❌ Error:", e)
        return jsonify({'error': str(e)}), 500


# 2️⃣ Add feature to a plan
@license_bp.route('/add_license_feature', methods=['POST'])
def add_license_feature():
    data = request.json
    plan_id = data.get('plan_id')
    feature_name = data.get('feature_name')
    is_enabled = data.get('is_enabled', True)

    print("⚙️ Adding Feature:", feature_name, "to Plan ID:", plan_id)

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO license_features (plan_id, feature_name, is_enabled)
            VALUES (%s, %s, %s)
        """, (plan_id, feature_name, is_enabled))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'message': 'Feature added successfully'}), 200
    except Exception as e:
        print("❌ Error:", e)
        return jsonify({'error': str(e)}), 500


# 3️⃣ Fetch all plans with their features
@license_bp.route('/get_all_plans', methods=['POST'])
def get_all_plans():
    print("📄 Fetching all license plans...")
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)
        cur.execute("""
            SELECT lp.id, lp.plan_name, lp.description, lp.storage_limit, lp.price,
                   json_agg(json_build_object('feature_name', lf.feature_name, 'is_enabled', lf.is_enabled))
            AS features
            FROM license_plan lp
            LEFT JOIN license_features lf ON lp.id = lf.plan_id
            GROUP BY lp.id;
        """)
        plans = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({'message': 'License plans fetched successfully', 'data': plans}), 200
    except Exception as e:
        print("❌ Error:", e)
        return jsonify({'error': str(e)}), 500


# 4️⃣ Assign a license plan to an organization
@license_bp.route('/assign_license', methods=['POST'])
def assign_license():
    data = request.json
    organization_id = data.get('organization_id')
    plan_id = data.get('plan_id')
    start_date = data.get('start_date')
    end_date = data.get('end_date')

    print(f"🏢 Assigning Plan {plan_id} to Organization {organization_id}")

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO organization_license (organization_id, plan_id, start_date, end_date)
            VALUES (%s, %s, %s, %s)
        """, (organization_id, plan_id, start_date, end_date))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'message': 'License assigned successfully'}), 200
    except Exception as e:
        print("❌ Error:", e)
        return jsonify({'error': str(e)}), 500
