from google.cloud import storage
try:
    client = storage.Client()
    buckets = list(client.list_buckets())
    print("Connexion réussie ! Voici tes buckets :", buckets)
except Exception as e:
    print("Erreur d'auth :", e) 