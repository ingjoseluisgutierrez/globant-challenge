from app.db import engine, Base
import app.models

print("Creando tablas en:", engine.url)
Base.metadata.create_all(bind=engine)
print("Done: tablas creadas (si no existían).")
