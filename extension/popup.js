window.onload = function() {
    var  repo = chrome.tabs.getSelected(null, function(tab) {
        var tabUrl = tab.url;
        let splitURL = tabUrl.split("/");
        let userName = splitURL[3];
        let repoName = splitURL[4];
        let fullName = userName + "%2F" + repoName;
        fetch('https://githubmeta.azurewebsites.net/api/summarize?repo=' + fullName)
            .then(response => response.json())
            .then(data => {
                str = ""
                for ((key, value) in data) {
                    str += "<bold>" + key + "</bold>: " + value + "<br>"
                }
                document.getElementById('response').textContent = str;
                });
    });
};
