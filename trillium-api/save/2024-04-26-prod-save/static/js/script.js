document.addEventListener('DOMContentLoaded', function() {
    fetchLeaderboard();
    fetchPubkeys();
    populateFilterRanges();

    const flatpickrOptions = {
        enableTime: true,
        dateFormat: "Y-m-d H:i",
        time_24hr: true,
    };

    flatpickr("#start-time", flatpickrOptions);

    document.getElementById('filter-form').addEventListener('submit', function(e) {
        e.preventDefault();

        const filterType = e.submitter.value;

        if (filterType === 'pubkey') {
            const pubkeys = [
                document.getElementById('pubkey1').value,
                document.getElementById('pubkey2').value,
                document.getElementById('pubkey3').value,
                document.getElementById('pubkey4').value,
                document.getElementById('pubkey5').value
            ].filter(pubkey => pubkey !== '');

            fetchLeaderboard({ pubkeys });
        } else if (filterType === 'time') {
            const startTime = new Date(document.getElementById('start-time').value).getTime() / 1000;
            fetchLeaderboard({ startTime });
        } else if (filterType === 'slot') {
            const startSlot = document.getElementById('start-slot').value;
            const endSlot = document.getElementById('end-slot').value;
            fetchLeaderboard({ startSlot, endSlot });
        } else if (filterType === 'epoch') {
            const epoch1 = document.getElementById('epoch1').value;
            const epoch2 = document.getElementById('epoch2').value;
            fetchLeaderboard({ epoch1, epoch2 });
        }
    });
});

function fetchLeaderboard(filters = {}) {
    const url = `/leaderboard_viz?${new URLSearchParams(filters)}`;

    fetch(url)
        .then(response => response.text())
        .then(html => {
            console.log('Response HTML:', html);
            const leaderboardDiv = document.getElementById('leaderboard');
            leaderboardDiv.innerHTML = html;
        })
        .catch(error => {
            console.error('Error fetching leaderboard:', error);
        });
}

function fetchPubkeys() {
    fetch('/pubkeys')
        .then(response => response.json())
        .then(data => {
            const pubkeyList = document.getElementById('pubkey-list');
            pubkeyList.innerHTML = '';
            data.pubkeys.forEach(pubkey => {
                const option = document.createElement('option');
                option.value = pubkey;
                pubkeyList.appendChild(option);
            });
        });
}

function populateFilterRanges() {
    fetch('/leaderboard_viz')
        .then(response => response.json())
        .then(data => {
            document.getElementById('start-time').value = data.min_time;
            document.getElementById('start-slot').placeholder = `Min: ${data.min_slot}`;
            document.getElementById('end-slot').placeholder = `Max: ${data.max_slot}`;
            document.getElementById('epoch1').placeholder = `Min: ${data.min_epoch}`;
            document.getElementById('epoch2').placeholder = `Max: ${data.max_epoch}`;
        });
}