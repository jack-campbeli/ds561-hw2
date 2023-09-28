from google.cloud import storage
from bs4 import BeautifulSoup
import numpy as np
from tqdm import tqdm

# Replace with your Google Cloud Storage bucket and project ID
bucket_name = 'bu-ds561-jawicamp'
project_id = 'jacks-project-398813'

# incoming and outgoing links counts + graph
incoming_links_counts = {}
outgoing_links_counts = {}
graph = {}

# Create a list of all files in the bucket
def list_bucket_files(bucket_name, project_id):
    try:
        # Initialize client
        client = storage.Client(project=project_id)

        # Get bucket
        bucket = client.get_bucket(bucket_name)

        # List all files in the bucket
        blobs = list(bucket.list_blobs())

        return blobs
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return []

def parse_file(blob):
    try:
        outgoing_links_list = []
        # get all anchor tags from the HTML file/blob
        html_content = blob.download_as_text()
        soup = BeautifulSoup(html_content, 'html.parser')
        anchor_tags = soup.find_all('a')

        # iterate over all anchor tags and get the href attribute
        for tag in anchor_tags:
            href = tag.get('href')
            if href:
                if href.endswith(".html"):
                    # increment the incoming link count for the associated file
                    incoming_links_counts[href] = incoming_links_counts.get(href, 0) + 1
                    # add link to the current page's outgoing links list
                    outgoing_links_list.append(href[:-5])

        # add outgoing links count for the current page
        outgoing_links_counts[blob.name] = len(outgoing_links_list)
        # add current page node and edges to the graph
        page_name = blob.name[:-5]
        if page_name not in graph:
            graph[page_name] = {'outgoing': outgoing_links_list, 'incoming': []}
        else:
            graph[page_name]['outgoing'] = outgoing_links_list

        # Update incoming links for linked pages
        for link in outgoing_links_list:
            if link not in graph:
                graph[link] = {'outgoing': [], 'incoming': [page_name]}
            else:
                graph[link]['incoming'].append(page_name)

    except Exception as e:
        print(f"An error occurred while parsing: {str(e)}")

def compute_average_links(link_counts):
    total_links = 0
    total_files = 0

    for file, count in link_counts.items():
        total_links += count
        total_files += 1

    return total_links / total_files

def pagerank_iterative(graph, pagerank_prev, convergence_threshold):
    while True:
        # new pagerank iteration
        pagerank_curr = {}

        for page in graph:
            pagerank_curr[page] = 0.15  # Initialize with the damping factor

            # for each incoming link, add the pagerank of the incoming page divided by the number of outgoing links
            incoming_links = graph[page]['incoming']
            for incoming_page in incoming_links:
                pagerank_curr[page] += 0.85 * (pagerank_prev.get(incoming_page, 0.0) / len(graph[incoming_page]['outgoing']))

        # normalize the pagerank values
        total_pagerank = sum(pagerank_curr.values())
        pagerank_curr = {page: pagerank / total_pagerank for page, pagerank in pagerank_curr.items()}

        # calculate convergence
        difference = sum(pagerank_curr.values()) - sum(pagerank_prev.values())

        # update pagerank_prev with the new values
        pagerank_prev = pagerank_curr.copy()

        # check for convergence
        if abs(difference) < convergence_threshold:
            return pagerank_curr.copy()

if __name__ == '__main__':
    blobs = list_bucket_files(bucket_name, project_id)

    if blobs:
        print(f"List of files in bucket '{bucket_name}':")
        # max_files = len(blobs)
        max_files = 500

        pagerank_prev = {}

        # iterate over all files and populate the graph and relative statistics
        for blob in tqdm(blobs, total=max_files):
            if blob.name.endswith(".html") and max_files > 0:
                max_files -= 1
                # initialize pagerank values
                pagerank_prev[blob.name[:-5]] = 0.01
                parse_file(blob)

        pagerank = pagerank_iterative(graph, pagerank_prev, 0.005)

        # PageRank Iterative Approach
        print()
        print("--------------------------------------------------")
        print("PageRank Iterative (Top 5):")
        top_pages = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:5]
        for rank, (page, score) in enumerate(top_pages, start=1):
            print(f"Rank {rank}: {page}, Score {score:.12f}")

        print("--------------------------------------------------")

        # Incoming Links Statistics
        print("Incoming links statistics:")
        print(f"Average Incoming Links: {compute_average_links(incoming_links_counts):.4f}")
        print("Max Incoming:", max(incoming_links_counts.values()))
        print("Min Incoming:", min(incoming_links_counts.values()))
        print("Median Incoming:", np.median(list(incoming_links_counts.values())))

        incoming_quintiles = np.percentile(list(incoming_links_counts.values()), [20, 40, 60, 80])
        print("Quintiles:")
        print(f"20th Percentile: {incoming_quintiles[0]:.4f}")
        print(f"40th Percentile: {incoming_quintiles[1]:.4f}")
        print(f"60th Percentile: {incoming_quintiles[2]:.4f}")
        print(f"80th Percentile: {incoming_quintiles[3]:.4f}")

        print("--------------------------------------------------")

        # Outgoing Links Statistics
        print("Outgoing links statistics:")
        print(f"Average Outgoing Links: {compute_average_links(outgoing_links_counts):.4f}")
        print("Max Outgoing:", max(outgoing_links_counts.values()))
        print("Min Outgoing:", min(outgoing_links_counts.values()))
        print("Median Outgoing:", np.median(list(outgoing_links_counts.values())))

        outgoing_quintiles = np.percentile(list(outgoing_links_counts.values()), [20, 40, 60, 80])
        print("Quintiles:")
        print(f"20th Percentile: {outgoing_quintiles[0]:.4f}")
        print(f"40th Percentile: {outgoing_quintiles[1]:.4f}")
        print(f"60th Percentile: {outgoing_quintiles[2]:.4f}")
        print(f"80th Percentile: {outgoing_quintiles[3]:.4f}")

    else:
        print("No files found in the bucket.")
