// keep-dropdown-open.js
(function() {
    // Wait until the document is fully loaded
    document.addEventListener('DOMContentLoaded', function() {
        // Find the dropdown menu
        let dropdownMenu = document.querySelector('.dropdown-menu.show');
        
        // Add an event listener for clicks
        document.addEventListener('click', function(e) {
            // Only apply to multi-select dropdowns
            let isDropdown = e.target.closest('.dropdown-menu');
            if (isDropdown && isDropdown.classList.contains('show')) {
                // Reopen the dropdown menu
                setTimeout(function() {
                    dropdownMenu.classList.add('show');
                }, 0);
            }
        });
    });
})();
