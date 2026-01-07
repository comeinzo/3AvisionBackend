from flask_socketio import SocketIO

# Shared SocketIO instance to avoid circular imports
socketio = SocketIO(cors_allowed_origins="*")