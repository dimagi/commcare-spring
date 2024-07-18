package org.commcare.formplayer.session

import org.commcare.cases.util.StringUtils
import org.commcare.formplayer.beans.menus.PeristentCommand
import org.commcare.util.screen.EntityScreen
import org.commcare.util.screen.MenuScreen
import org.commcare.suite.model.MenuDisplayable;
import org.commcare.modern.session.SessionWrapper;
import org.commcare.util.screen.MultiSelectEntityScreen
import org.commcare.util.screen.Screen

/**
 * Utility methods related to persistent menu evaluation
 */
class PersistentMenuHelper(val isPersistentMenuEnabled: Boolean) {

    var persistentMenu: ArrayList<PeristentCommand> = ArrayList()
    private var currentMenu: PeristentCommand? = null

    fun addEntitySelection(input: String, entityText: String) {
        if (isPersistentMenuEnabled && !StringUtils.isEmpty(input) && !input.contentEquals(MultiSelectEntityScreen.USE_SELECTED_VALUES)) {
            val command = PeristentCommand(input, entityText)
            addPersistentCommand(command)
        }
    }

    fun addMenusToPeristentMenu(menuScreen: MenuScreen, sessionWrapper: SessionWrapper) {
        if (isPersistentMenuEnabled) {
            val options = menuScreen.getOptions()
        val mChoices = menuScreen.getMenuDisplayables()
            for (i in options.indices) {
                val command = PeristentCommand(i.toString(), options[i], mChoices[i], sessionWrapper)
                addPersistentCommand(command)
            }
        }
    }

    /**
     * Identifies and sets current menu based on the input give to the current screen
     */
     fun advanceCurrentMenuWithInput(screen: Screen, input: String) {
        if (isPersistentMenuEnabled) {
            if (screen is MenuScreen) {
                val index = input.toInt()
                val parentMenu = if (currentMenu == null) persistentMenu else currentMenu!!.commands
                if (index < parentMenu.size) {
                    currentMenu = parentMenu[index]
                }
            } else if (screen is EntityScreen) {
                check(currentMenu != null) { "Current menu can't be null for Entity screen" }
                check(currentMenu!!.commands.size <= 1) { "Current menu can't have more than one commands for Entity screen" }

                // if it's the last input, we would not have yet added entity screen menu to peristent Menu, so just
                // return current menu otherwise return the only command entry for entity screen
                currentMenu = if (currentMenu!!.commands.size == 0) currentMenu else currentMenu!!.commands[0]
            }
        }
    }

    private fun addPersistentCommand(command: PeristentCommand) {
        // currentMenu!=null implies that we must have added items to persistent menu
        check(currentMenu == null || persistentMenu.size > 0)
        if (currentMenu == null) {
            persistentMenu.add(command)
        } else {
            currentMenu!!.addCommand(command)
        }
    }
}
