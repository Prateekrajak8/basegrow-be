import datetime

import bcrypt
import jwt
from django.conf import settings
from django.db import connection
from rest_framework.decorators import api_view
from rest_framework.response import Response


def _fetch_user_by_email(email: str):
    with connection.cursor() as cursor:
        cursor.execute('SELECT id, email, password FROM "User" WHERE email = %s LIMIT 1', [email])
        row = cursor.fetchone()

    if not row:
        return None

    return {
        "id": row[0],
        "email": row[1],
        "password": row[2],
    }


@api_view(["POST"])
def login_view(request):
    try:
        email = request.data.get("email")
        password = request.data.get("password")

        if not email or not password:
            return Response({"error": "Email and password are required"}, status=400)

        user = _fetch_user_by_email(email)
        if not user:
            return Response({"error": "User not found"}, status=404)

        hashed = user["password"]
        if not bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8")):
            return Response({"error": "Invalid credentials"}, status=401)

        token = jwt.encode(
            {
                "userId": user["id"],
                "email": user["email"],
                "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=12),
            },
            settings.JWT_SECRET,
            algorithm="HS256",
        )

        return Response(
            {
                "token": token,
                "user": {
                    "id": user["id"],
                    "email": user["email"],
                },
            },
            status=200,
        )
    except Exception:
        return Response({"error": "Internal server error"}, status=500)


@api_view(["POST"])
def register_view(request):
    try:
        email = request.data.get("email")
        password = request.data.get("password")

        if not email or not password:
            return Response({"error": "Email and password are required"}, status=400)

        email = email.strip().lower()

        existing = _fetch_user_by_email(email)
        if existing:
            return Response({"error": "User already exists"}, status=409)

        hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        now = datetime.datetime.utcnow()

        with connection.cursor() as cursor:
            cursor.execute(
                'INSERT INTO "User" (email, password, "createdAt") VALUES (%s, %s, %s) RETURNING id',
                [email, hashed, now],
            )
            user_id = cursor.fetchone()[0]

        token = jwt.encode(
            {
                "userId": user_id,
                "email": email,
                "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=12),
            },
            settings.JWT_SECRET,
            algorithm="HS256",
        )

        return Response(
            {
                "token": token,
                "user": {
                    "id": user_id,
                    "email": email,
                },
            },
            status=201,
        )
    except Exception:
        return Response({"error": "Internal server error"}, status=500)
