from flask import Flask, render_template, request
from search_engine import search_engine

app = Flask(__name__)

search_engine = search_engine()

@app.route('/', methods = ['GET', 'POST'])
def index():
    search_results = []
    if request.method == 'POST':
        query = request.form.get('query', '')
    else:
        query = request.args.get('query', '')
    page = int(request.args.get('page', 1))
    per_page = 10
    print(query)
    print(page)

    if query:
        search_results = search_engine.search(query)

    total_results = len(search_results)
    paginated_results = search_results[(page - 1) * per_page: page * per_page]
    total_pages = (total_results// per_page) + (1 if total_results % per_page > 0 else 0)
    print(total_pages)

    return render_template(
        'index.html',
        query = query,
        search_results = paginated_results,
        current_page = page,
        total_pages = total_pages,
        total_results = total_results
    )


if __name__ == '__main__':
    app.run(debug = True)