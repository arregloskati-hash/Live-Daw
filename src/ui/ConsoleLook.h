#pragma once

#include "engine/ConsoleModel.h"

#include <juce_data_structures/juce_data_structures.h>

namespace livedaw::ui
{

/** The control surface's finish: Behringer makes the full-size WING in
    silver and in black (WING BK). Only the surface view's hardware colours
    differ; the layout, the print and the touchscreen are the same. */
enum class SurfaceFinish
{
    Silver,
    Black,
    Matte // DARK appearance: a matte black surface, charcoal decks, grey keys, white fader caps
};

/** How the console looks (SETUP > GENERAL > CONSOLE), kept in the app
    settings rather than in the show: a preference of the user, not of the mix.
    The MODEL is the full-size WING or the WING COMPACT (the RACK may follow);
    the COMPACT comes in black only, so it shows the black finish whatever the
    finish setting says. */
struct ConsoleLook
{
    static constexpr const char* finishKey = "ui.surfaceFinish"; // "MATTE" (default, DARK), "SILVER" (WHITE) or "BLACK" (BK)
    static constexpr const char* modelKey = "ui.consoleModel";   // "FULL" (default) or "COMPACT"

    SurfaceFinish finish = SurfaceFinish::Matte;
    ConsoleModel model = ConsoleModel::Full;

    static juce::String finishName (SurfaceFinish f)
    {
        switch (f)
        {
            case SurfaceFinish::Silver: return "SILVER";
            case SurfaceFinish::Black: return "BLACK";
            case SurfaceFinish::Matte: break;
        }
        return "MATTE";
    }

    static SurfaceFinish finishFromName (const juce::String& name)
    {
        if (name == finishName (SurfaceFinish::Silver)) return SurfaceFinish::Silver;
        if (name == finishName (SurfaceFinish::Black)) return SurfaceFinish::Black;
        return SurfaceFinish::Matte; // the default (and anything unknown)
    }

    /** The finish the surface shows: the COMPACT has no silver one, so it
        shows black then. */
    SurfaceFinish shownFinish() const noexcept
    {
        return model == ConsoleModel::Compact && finish == SurfaceFinish::Silver ? SurfaceFinish::Black : finish;
    }

    static ConsoleLook load (const juce::PropertiesFile& settings)
    {
        ConsoleLook look;
        look.finish = finishFromName (settings.getValue (finishKey));
        look.model = consoleModelFromName (settings.getValue (modelKey));
        return look;
    }

    void save (juce::PropertiesFile& settings) const
    {
        settings.setValue (finishKey, finishName (finish));
        settings.setValue (modelKey, consoleModelName (model));
    }
};

} // namespace livedaw::ui
